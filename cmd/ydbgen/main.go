package main

import (
	"bytes"
	"container/list"
	"flag"
	"fmt"
	"go/ast"
	"go/build"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/yandex-cloud/ydb-go-sdk/internal"
	"github.com/yandex-cloud/ydb-go-sdk/internal/ydbtypes"
)

func main() {
	log.SetFlags(0)
	log.SetPrefix("ydbgen: ")

	var (
		wrapMode = flag.String("wrap", "optional", "default type wrapping mode")
		convMode = flag.String("conv", "safe", "default conv mode")
		seekMode = flag.String("seek", "column", "default seek mode")

		dir     = flag.String("dir", "", "directory to generate code for")
		out     = flag.String("out", "", "directory to put results to")
		exclude = flag.String("exclude", "", "regular expression to exclude files from build")
		gentype = flag.String("type", "", "comma-separated list of types to generate code for")
		all     = flag.Bool("all", false, "generate code for all found types")

		goroot       = flag.String("goroot", "", "replace go/build GOROOT path")
		sourceLookup = flag.String("lookup", "", "mapping of base import path to directory in form of import:dir")

		force   = flag.Bool("force", false, "ignore type errors")
		verbose = flag.Bool("verbose", false, "print debug info")
	)
	flag.Parse()

	// TODO(kamardin): add default GenFlags to use with `-all` flag for example.
	var DefaultMode GenMode
	{
		var err error
		DefaultMode.Wrap, err = ParseWrapMode(*wrapMode)
		if err != nil {
			log.Fatal(err)
		}
		DefaultMode.Conv, err = ParseConvMode(*convMode)
		if err != nil {
			log.Fatal(err)
		}
		DefaultMode.Seek, err = ParseSeekMode(*seekMode)
		if err != nil {
			log.Fatal(err)
		}
	}

	workDir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	var (
		sourceDir = *dir
		outputDir = *out
	)
	for _, d := range []*string{&sourceDir, &outputDir} {
		if *d == "" {
			*d = "."
		}
		if !path.IsAbs(*d) {
			*d = path.Join(workDir, *d)
		}
	}

	var (
		processFile = func(string) bool { return true }
		excludeFile = func(string) bool { return false }
	)
	if gofile := os.Getenv("GOFILE"); gofile != "" && *dir == "" {
		// Running from `go generate` and no dir is given.
		// Thus, process only marked file.
		want := path.Join(sourceDir, gofile)
		processFile = func(name string) bool {
			return name == want
		}
	}
	if *exclude != "" {
		re, err := regexp.Compile(*exclude)
		if err != nil {
			log.Fatalf("compile ignore regexp error: %v", err)
		}
		excludeFile = re.MatchString
	}
	matches, err := filepath.Glob(path.Join(sourceDir, "*.go"))
	if err != nil {
		log.Fatal(err)
	}

	wantType := map[string]bool{}
	for _, t := range strings.Split(*gentype, ",") {
		wantType[t] = true
	}

	var (
		files    = make([]*os.File, 0, len(matches))
		astFiles = make([]*ast.File, 0, len(matches))
	)
	fset := token.NewFileSet()
	for _, fpath := range matches {
		if strings.HasSuffix(fpath, GeneratedFileSuffix+".go") {
			continue
		}
		if excludeFile(fpath) {
			continue
		}

		file, err := os.Open(fpath)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		// Parse the input string, []byte, or io.Reader,
		// recording position information in fset.
		// ParseFile returns an *ast.File, a syntax tree.
		f, err := parser.ParseFile(fset, file.Name(), file, parser.ParseComments)
		if err != nil {
			log.Fatalf("parse %q error: %v", file.Name(), err)
		}

		files = append(files, file)
		astFiles = append(astFiles, f)
	}
	if *sourceLookup != "" {
		// We are given some base import path to be searchable by type checker.
		// That is, we expand default GOPATH variable with temporary directory
		// with GOPATH friendly layout which src directory contains only base
		// import path node which is a symbolic link to the given source base
		// directory.
		var paths []string
		for _, pair := range strings.Split(*sourceLookup, ",") {
			tmp, err := ioutil.TempDir("", "ydbgen")
			if err != nil {
				log.Fatal(err)
			}
			defer os.RemoveAll(tmp)
			if err := os.Mkdir(path.Join(tmp, "src"), 0700); err != nil {
				log.Fatal(err)
			}
			base, dir := splitPair(strings.TrimSpace(pair), ':')
			err = os.Symlink(dir, path.Join(tmp, "src", base))
			if err != nil {
				log.Fatal(err)
			}
			paths = append(paths, tmp)
		}
		if dir := os.Getenv("GOPATH"); dir != "" {
			paths = append(paths, dir)
		}
		build.Default.GOPATH = strings.Join(
			paths, string(filepath.ListSeparator),
		)
	}
	if *goroot != "" {
		build.Default.GOROOT = *goroot
	}

	// A Config controls various options of the type checker.
	// The defaults work fine except for one setting:
	// we must specify how to deal with imports.
	conf := types.Config{
		IgnoreFuncBodies: true,
		Importer:         importer.ForCompiler(fset, "source", nil),
	}
	if *force {
		conf.Error = func(err error) {
			log.Printf("suppressing error: %v", err)
		}
	}
	// Type-check the package containing only file f.
	// Check returns a *types.Package.
	info := types.Info{
		// Query types information to this mapping.
		Types: make(map[ast.Expr]types.TypeAndValue),
		Defs:  make(map[*ast.Ident]types.Object),
		Uses:  make(map[*ast.Ident]types.Object),
	}
	p, err := conf.Check(".", fset, astFiles, &info)
	if err != nil && !*force {
		log.Fatalf("type error: %v", err)
	}
	pkg := Package{
		Name: p.Name(),
	}
	for i, astFile := range astFiles {
		var (
			depth int
			reset int
			items []*GenItem
			item  *GenItem

			astLog = func(s string, args ...interface{}) {
				if *verbose {
					log.Print(strings.Repeat(" ", depth*4), fmt.Sprintf(s, args...))
				}
			}
		)
		if name := files[i].Name(); !processFile(name) {
			continue
		}
		ast.Inspect(astFile, func(n ast.Node) (dig bool) {
			astLog("%T", n)
			if depth == reset && item != nil {
				astLog("reset item")
				item = nil
				reset = -1
			}
			if n == nil {
				// Reached the end of current node digging.
				depth--
				return true
			}
			defer func() {
				if dig {
					depth++
				}
			}()

			switch v := n.(type) {
			case
				*ast.FuncDecl,
				*ast.ValueSpec:
				return false

			case *ast.Ident:
				astLog("ident %q", v.Name)
				if item != nil {
					item.Ident = v
				}
				return false

			case *ast.CommentGroup:
				for _, c := range v.List {
					astLog("comment %q", c.Text)

					text := strings.TrimPrefix(c.Text, "//ydb:")
					if c.Text != text {
						if item == nil {
							item = &GenItem{
								File: files[i],
								Mode: DefaultMode,
							}
						}
						if err := item.ParseComment(text); err != nil {
							log.Fatalf(
								"malformed comment string: %q: %v",
								text, err,
							)
						}
					}
				}
				return false

			case *ast.TypeSpec:
				if item == nil && (*all || wantType[v.Name.Name]) {
					item = &GenItem{
						File:  files[i],
						Mode:  DefaultMode,
						Flags: GenAll,
					}
				}
				if item == nil {
					astLog("skipping type spec %q", v.Name)
					return false
				}
				item.TypeSpec = v
				reset = depth
				astLog("processing type spec %q", v.Name)

			case *ast.StructType:
				astLog("struct %+v", v)
				item.StructType = v
				items = append(items, item)
				item = nil
				reset = -1
				return false

			case *ast.ArrayType:
				if v.Len != nil {
					// Type is not a slice.
					astLog("skipping array type")
					return false
				}
				item.ArrayType = v
				items = append(items, item)
				item = nil
				reset = -1
				return false
			}

			return true
		})

		var fillSlice func(*ast.ArrayType, *Slice) types.Type
		fillSlice = func(arr *ast.ArrayType, dest *Slice) (base types.Type) {
			defer func() {
				base = types.NewSlice(base)
			}()
			typ := info.TypeOf(arr.Elt)
			switch x := typ.(type) {
			case *types.Basic:
				t := &TypeInfo{
					Type: x,
				}
				t.Primitive, err = ydbtypes.PrimitiveTypeFromGoType(typ)
				if err != nil {
					log.Fatal(err)
				}
				t.BaseType = ydbtypes.GoTypeFromPrimitiveType(t.Primitive)
				dest.Basic = t
				return t.BaseType

			case *types.Named:
				pkg.Couple(x.Obj().Id(), func(x interface{}) {
					dest.Struct = x.(*Struct)
				})
				return x.Underlying()

			case *types.Slice:
				t := &TypeInfo{
					Type:      x,
					Container: true,
					Slice:     new(Slice),
				}
				b := fillSlice(arr.Elt.(*ast.ArrayType), t.Slice)
				dest.Basic = t
				return types.NewSlice(b)

			default:
				log.Fatalf("unsupported type for slice element: %T (%[1]s)", x)
				return nil
			}
		}

		file := &File{
			Name: files[i].Name(),
		}
		for _, item := range items {
			switch {
			case item.ArrayType != nil:
				s := &Slice{
					Name:  item.Ident.Name,
					Flags: item.Flags,
				}
				fillSlice(item.ArrayType, s)
				file.Slices = append(file.Slices, s)

			case item.StructType != nil:
				s := &Struct{
					Name:  item.Ident.Name,
					Flags: item.Flags,
				}

				pkg.Register(item.Ident.Name, s)
				decl := info.TypeOf(item.StructType).(*types.Struct)

				for i, f := range item.StructType.Fields.List {
					name := f.Names[0].Name
					field := &Field{
						TypeInfo: TypeInfo{
							Conv: item.Mode.Conv,
						},
						Name:     name,
						Column:   camelToSnake(name),
						Position: i,
					}
					if err := field.ParseTags(decl.Tag(i)); err != nil {
						log.Fatal(err)
					}
					if field.Ignore {
						continue
					}

					var (
						typ types.Type
						arr *ast.ArrayType
					)
					switch x := f.Type.(type) {
					case *ast.SelectorExpr:
						obj := info.ObjectOf(x.Sel)
						typ = obj.Type()

					case *ast.Ident:
						typ = info.TypeOf(f.Type)

					case *ast.ArrayType:
						typ = info.TypeOf(f.Type)
						arr = x

					default:
						log.Fatalf(
							"unexpected field %q ast type: %T",
							field.Name, f.Type,
						)
					}

					switch x := typ.(type) {
					case *types.Slice:
						field.Type = x
						if field.Primitive == 0 {
							// No "type" tag specified.
							field.Container = true
							field.Slice = &Slice{}
							field.BaseType = fillSlice(arr, field.Slice)
						}

					case *types.Basic:
						field.Type = x

					case *types.Named:
						obj := x.Obj()
						name := strings.Join([]string{
							obj.Pkg().Name(),
							obj.Name(),
						}, ".")
						switch {
						case name == "time.Time":
							if field.BaseType == nil {
								log.Fatalf(
									"%s.%s must have type tag specified (necessary for %s conversions)",
									s.Name, field.Name, name,
								)
							}
							field.Type = field.BaseType
							field.Face = TimeFieldFace{}

						default:
							t, err := checkInterface(x, s.Flags)
							if err != nil {
								field.Container = true
								pkg.Couple(obj.Name(), func(x interface{}) {
									field.Struct = x.(*Struct)
								})
							} else {
								field.Type = t
								field.Face = DefaultFieldFace{}
							}
						}

					default:
						log.Fatalf("unexpected field object type: %T", x)
					}

					if !field.Container && field.Primitive == 0 {
						var err error
						field.Primitive, err = ydbtypes.PrimitiveTypeFromGoType(field.Type)
						if err != nil {
							log.Fatal(err)
						}
						field.Optional = item.Mode.Wrap == WrapOptional
						field.BaseType = ydbtypes.GoTypeFromPrimitiveType(field.Primitive)
					}
					if err := field.Validate(); err != nil {
						log.Fatalf("generate struct %q error: %v", s.Name, err)
					}

					s.Fields = append(s.Fields, field)
				}
				if s.SeekMode == SeekPosition {
					sort.Slice(s.Fields, func(i, j int) bool {
						return s.Fields[i].Position < s.Fields[j].Position
					})
				}
				if s.SeekMode == SeekUnknown {
					s.SeekMode = item.Mode.Seek
				}

				file.Structs = append(file.Structs, s)
			}
		}

		pkg.Files = append(pkg.Files, file)
	}

	if err := pkg.Finalize(); err != nil {
		log.Fatal(err)
	}

	g := Generator{
		Dir: outputDir,
	}
	if err := g.Generate(pkg); err != nil {
		log.Fatal(err)
	}
}

type SeekMode uint

const (
	SeekUnknown SeekMode = iota
	SeekPosition
	SeekColumn
)

func ParseSeekMode(s string) (SeekMode, error) {
	switch s {
	case "position":
		return SeekPosition, nil
	case "column":
		return SeekColumn, nil
	default:
		return 0, fmt.Errorf("unknown seek mode: %q", s)
	}
}

type Package struct {
	Name  string
	Files []*File

	Known     map[string]interface{}
	Container map[string]*list.List
}

func (p *Package) Couple(id string, f func(interface{})) {
	if x, ok := p.Known[id]; ok {
		f(x)
		return
	}
	if p.Container == nil {
		p.Container = make(map[string]*list.List)
	}
	if p.Container[id] == nil {
		p.Container[id] = list.New()
	}
	p.Container[id].PushBack(f)
}

func (p *Package) Register(id string, x interface{}) {
	if p.Known == nil {
		p.Known = make(map[string]interface{})
	}
	p.Known[id] = x

	list := p.Container[id]
	if list == nil {
		return
	}

	delete(p.Container, id)
	for el := list.Front(); el != nil; el = el.Next() {
		el.Value.(func(interface{}))(x)
	}
}

func (p *Package) Finalize() error {
	for id, list := range p.Container {
		return fmt.Errorf(
			"type dependency not met: %d container type(s) want type %s to be generated",
			list.Len(), id,
		)
	}
	return nil
}

type WrapMode uint

const (
	WrapModeUnknown WrapMode = iota
	WrapOptional
	WrapNothing
)

func ParseWrapMode(s string) (WrapMode, error) {
	switch s {
	case "optional":
		return WrapOptional, nil
	case "none":
		return WrapNothing, nil
	default:
		return 0, fmt.Errorf("unknown type mode: %q", s)
	}
}

type GenMode struct {
	Wrap WrapMode
	Seek SeekMode
	Conv ConvMode
}

type GenItem struct {
	File       *os.File
	Ident      *ast.Ident
	TypeSpec   *ast.TypeSpec
	StructType *ast.StructType
	ArrayType  *ast.ArrayType

	Flags GenFlag
	Mode  GenMode
}

func (g *GenItem) ParseComment(text string) (err error) {
	prefix, text := splitPair(text, ' ')
	switch prefix {
	case "gen", "generate":
		return g.parseGenFlags(text)
	case "set":
		return g.parseGenMode(text)
	default:
		return fmt.Errorf("unkown prefix: %q", prefix)
	}
}

func (g *GenItem) parseGenFlags(text string) error {
	for _, param := range strings.Split(text, ",") {
		switch param {
		case "":
			g.Flags = GenAll
		case "scan":
			g.Flags |= GenScan
		case "params":
			g.Flags |= GenQueryParams
		case "value":
			g.Flags |= GenValue
		case "type":
			g.Flags |= GenType
		default:
			return fmt.Errorf("unknown generation flag: %q", param)
		}
	}
	return nil
}

func (g *GenItem) parseGenMode(text string) (err error) {
	for _, pair := range strings.Split(text, " ") {
		key, val := splitPair(strings.TrimSpace(pair), ':')
		switch key {
		case "wrap":
			g.Mode.Wrap, err = ParseWrapMode(val)
		case "seek":
			g.Mode.Seek, err = ParseSeekMode(val)
		case "conv":
			g.Mode.Conv, err = ParseConvMode(val)
		default:
			return fmt.Errorf("unknown option: %q", key)
		}
	}
	return
}

type GenFlag uint

const (
	GenNothing GenFlag = 1 << iota >> 1
	GenScan
	GenQueryParams
	GenValue
	GenType

	GenGet = GenValue | GenQueryParams
	GenSet = GenScan

	GenAll = ^GenFlag(0)
)

type File struct {
	Name    string
	Structs []*Struct
	Slices  []*Slice
}

func (f *File) Empty() bool {
	return len(f.Structs) == 0 && len(f.Slices) == 0
}

type Struct struct {
	Name     string
	Fields   []*Field
	SeekMode SeekMode
	Flags    GenFlag
}

type Slice struct {
	Name  string
	Flags GenFlag

	Basic  *TypeInfo
	Struct *Struct
}

type TypeInfo struct {
	Conv      ConvMode
	Face      FieldFace
	Type      types.Type // Actual go type.
	BaseType  types.Type // Column's go type.
	Primitive internal.PrimitiveType
	Optional  bool

	Container bool
	Struct    *Struct
	Slice     *Slice
}

type Field struct {
	TypeInfo

	Name     string
	Column   string
	Position int
	Ignore   bool
}

type ConvMode uint

const (
	ConvDefault = iota
	ConvUnsafe
	ConvAssert
)

func ParseConvMode(s string) (ConvMode, error) {
	switch s {
	case "safe":
		return ConvDefault, nil
	case "unsafe":
		return ConvUnsafe, nil
	case "assert":
		return ConvAssert, nil
	default:
		return 0, fmt.Errorf("unknown conv mode: %q", s)
	}
}

func isAssignable(t1, t2 types.Type) bool {
	s1, _ := t1.(*types.Slice)
	s2, _ := t2.(*types.Slice)
	if s1 != nil && s2 != nil {
		return isAssignable(s1.Elem(), s2.Elem())
	}
	return types.AssignableTo(t1, t2)
}

func (f *Field) Validate() error {
	if f.Container {
		if !isAssignable(f.Type, f.BaseType) {
			return fmt.Errorf(
				"field %q type %s is impossible to assign to %s",
				f.Name, f.Type, f.BaseType,
			)
		}
		return nil
	}
	if !types.ConvertibleTo(f.Type, f.BaseType) {
		return fmt.Errorf(
			"field %q impossible type conversion from %s to %s (ydb primitive type %s)",
			f.Name, f.Type, f.BaseType, f.Primitive,
		)
	}
	if f.Conv == ConvDefault {
		for _, conv := range [][2]types.Type{
			{f.BaseType, f.Type},
			{f.Type, f.BaseType},
		} {
			if !isSafeConversion(conv[0], conv[1]) {
				return fmt.Errorf(
					"unsafe type conversion for field %q: from %s to %s",
					f.Name, conv[0], conv[1],
				)
			}
		}
	}
	if f.Conv != ConvDefault {
		safe := true
		for _, conv := range [][2]types.Type{
			{f.BaseType, f.Type},
			{f.Type, f.BaseType},
		} {
			if !isSafeConversion(conv[0], conv[1]) {
				safe = false
				break
			}
		}
		if safe {
			return fmt.Errorf(
				"already safe type conversion for field %q: %s <-> %s",
				f.Name, f.Type, f.BaseType,
			)
		}
	}
	return nil
}

func (f *Field) ParseTags(tags string) (err error) {
	const tagPrefix = "ydb:"

	var value string
	for _, tag := range strings.Split(tags, " ") {
		if value = strings.TrimPrefix(tag, tagPrefix); value != tag {
			value = strings.Trim(value, `"`)
			break
		}
	}
	if value == "" {
		return nil
	}
	pairs := strings.Split(value, ",")
	var (
		columnGiven   bool
		positionGiven bool
	)
	for _, pair := range pairs {
		key, value := splitPair(pair, ':')
		if value == "" {
			if len(pairs) == 1 {
				// Special case when only column name or ignorance sign is given.
				if key == "-" {
					f.Ignore = true
				} else {
					f.Column = key
				}
				return
			}
			return fmt.Errorf("no value for tag key %q", key)
		}
		switch key {
		case "column":
			columnGiven = true
			f.Column = value
		case "pos":
			positionGiven = true
			f.Position, err = strconv.Atoi(value)
			if err != nil {
				return
			}
		case "type":
			if n := len(value); value[n-1] == '?' {
				f.Optional = true
				value = value[:n-1]
			}
			f.Primitive, err = ydbtypes.PrimitiveTypeFromString(value)
			if err != nil {
				return
			}
			f.BaseType = ydbtypes.GoTypeFromPrimitiveType(f.Primitive)
		case "conv":
			f.Conv, err = ParseConvMode(value)
			if err != nil {
				return
			}

		default:
			err = fmt.Errorf("unexpected tag key: %q", key)
			return
		}
	}
	if columnGiven && positionGiven {
		return fmt.Errorf(
			"ambiguous field %q parameters: column %q and position %d given",
			f.Name, f.Column, f.Position,
		)
	}
	return
}

func splitPair(p string, sep byte) (key, value string) {
	i := strings.IndexByte(p, sep)
	if i == -1 {
		return p, ""
	}
	return p[:i], p[i+1:]
}

func basic(t1, t2 types.Type) (b1, b2 *types.Basic, ok bool) {
	if b1, ok = t1.(*types.Basic); ok {
		b2, ok = t2.(*types.Basic)
	}
	return
}

// From t1 to t2.
func isSafeConversion(t1, t2 types.Type) bool {
	b1, b2, ok := basic(t1, t2)
	if !ok {
		// Let the go compiler to prepare type checking.
		return true
	}
	i1 := b1.Info()
	i2 := b2.Info()
	if i1&types.IsNumeric != i2&types.IsNumeric {
		// Let the go compiler to prepare type checking.
		return true
	}
	if i1&types.IsUnsigned != i2&types.IsUnsigned {
		return false
	}
	switch b1.Kind() {
	case types.Int, types.Uint:
		// int -> int64
		// uint -> uint64
		return sizeof(t2) == 64
	}
	switch b2.Kind() {
	case types.Int, types.Uint:
		// int{8,16,32} -> int
		// uint{8,16,32} -> uint
		return sizeof(t1) < 64
	}
	if sizeof(t1) > sizeof(t2) {
		return false
	}
	return true
}

func checkInterface(typ *types.Named, flags GenFlag) (_ *types.Basic, err error) {
	const (
		setter = "Set"
		getter = "Get"
	)
	var (
		getType *types.Basic
		setType *types.Basic
	)
	for i := 0; i < typ.NumMethods(); i++ {
		m := typ.Method(i)
		switch m.Name() {
		case getter:
			if flags&GenGet == 0 {
				continue
			}
			flags &= ^GenGet

			getType, err = resultWithFlag(m)
			if err != nil {
				return nil, err
			}
			if err = noParams(m); err != nil {
				return nil, err
			}

		case setter:
			if flags&GenSet == 0 {
				continue
			}
			flags &= ^GenSet

			setType, err = singleParam(m)
			if err != nil {
				return nil, err
			}
			if err = noResults(m); err != nil {
				return nil, err
			}
			if err = pointerReceiver(m); err != nil {
				return nil, err
			}

		default:
			continue
		}
	}
	if flags = flags & (GenGet | GenSet); flags != 0 {
		var buf bytes.Buffer
		if flags&GenGet != 0 {
			fmt.Fprintf(&buf, "\n\twant %s() (T, bool)", getter)
		}
		if flags&GenSet != 0 {
			fmt.Fprintf(&buf, "\n\twant %s(T)", setter)
		}
		return nil, fmt.Errorf("not enough methods: %s", buf.Bytes())
	}
	if getType != nil && setType != nil && getType != setType {
		return nil, fmt.Errorf(
			"getter and setter argument types are not equal: %s and %s",
			getType, setType,
		)
	}
	if getType != nil {
		return getType, nil
	}
	return setType, nil
}

func singleParam(f *types.Func) (*types.Basic, error) {
	var (
		s = f.Type().(*types.Signature)
		p = s.Params()
	)
	if n := p.Len(); n != 1 {
		return nil, fmt.Errorf(
			"unexpected method %q signature: have %d params; want 1",
			f.Name(), n,
		)
	}
	arg := p.At(0)
	if b, ok := arg.Type().(*types.Basic); ok {
		return b, nil
	}
	return nil, fmt.Errorf(
		"unexpected parameter %q of method %q type: "+
			"%s; only basic types are supported",
		f.Name(), arg.Name(), arg.Type(),
	)
}

func singleResult(f *types.Func) (*types.Basic, error) {
	var (
		s = f.Type().(*types.Signature)
		r = s.Results()
	)
	if n := r.Len(); n != 1 {
		return nil, fmt.Errorf(
			"unexpected method %q signature: have %d results; want 1",
			f.Name(), n,
		)
	}
	res := r.At(0)
	if b, ok := res.Type().(*types.Basic); ok {
		return b, nil
	}
	return nil, fmt.Errorf(
		"unexpected type of method %q result: "+
			"%s; only basic types are supported",
		f.Name(), res.Type(),
	)
}

func resultWithFlag(f *types.Func) (*types.Basic, error) {
	var (
		s = f.Type().(*types.Signature)
		r = s.Results()
	)
	if n := r.Len(); n != 2 {
		return nil, fmt.Errorf(
			"unexpected method %q signature: have %d results; want 2",
			f.Name(), n,
		)
	}

	res := r.At(0)
	rb, ok := res.Type().(*types.Basic)
	if !ok {
		return nil, fmt.Errorf(
			"unexpected type of method %q result value: "+
				"%s; only basic types are supported",
			f.Name(), res.Type(),
		)
	}

	flag := r.At(1)
	fb, ok := flag.Type().(*types.Basic)
	if !ok {
		return nil, fmt.Errorf(
			"unexpected type of method %q result value: "+
				"%s; only basic types are supported",
			f.Name(), flag.Type(),
		)
	}
	if fb.Kind() != types.Bool {
		return nil, fmt.Errorf(
			"unexpected type of method %q result flag: "+
				"have %s; want bool",
			f.Name(), fb.Name(),
		)
	}

	return rb, nil
}

func noParams(f *types.Func) error {
	var (
		s = f.Type().(*types.Signature)
		p = s.Params()
	)
	if n := p.Len(); n != 0 {
		return fmt.Errorf(
			"unexpected method %q signature: have %d param(s); want 0",
			f.Name(), n,
		)
	}
	return nil
}

func noResults(f *types.Func) error {
	var (
		s = f.Type().(*types.Signature)
		r = s.Results()
	)
	if n := r.Len(); n != 0 {
		return fmt.Errorf(
			"unexpected method %q signature: have %d result(s); want 0",
			f.Name(), n,
		)
	}
	return nil
}

func pointerReceiver(f *types.Func) error {
	var (
		s = f.Type().(*types.Signature)
		r = s.Recv()
	)
	if _, ptr := r.Type().(*types.Pointer); !ptr {
		return fmt.Errorf(
			"receiver of method %q must be a pointer",
			f.Name(),
		)
	}
	return nil
}
