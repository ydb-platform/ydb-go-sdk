package main

import (
	"container/list"
	"flag"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"log"
	"math/bits"
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
		ignorePat = flag.String("ignore", "", "regular expression to ignore files to scan")
	)
	flag.Parse()

	ignored := func(string) bool {
		return false
	}
	if *ignorePat != "" {
		re, err := regexp.Compile(*ignorePat)
		if err != nil {
			log.Fatalf("compile ignore regexp error: %v", err)
		}
		ignored = re.MatchString
	}

	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	matches, err := filepath.Glob(path.Join(wd, "*.go"))
	if err != nil {
		log.Fatal(err)
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
		if ignored(fpath) {
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
			log.Fatal(err)
		}

		files = append(files, file)
		astFiles = append(astFiles, f)
	}

	// A Config controls various options of the type checker.
	// The defaults work fine except for one setting:
	// we must specify how to deal with imports.
	conf := types.Config{
		Importer: importer.ForCompiler(fset, "source", nil),
	}
	// Type-check the package containing only file f.
	// Check returns a *types.Package.
	info := types.Info{
		// Query types information to this mapping.
		Types: make(map[ast.Expr]types.TypeAndValue),
	}
	p, err := conf.Check(".", fset, astFiles, &info)
	if err != nil {
		log.Fatal(err) // type error
	}
	pkg := Package{
		Name: p.Name(),
	}
	for i, astFile := range astFiles {
		type genItem struct {
			ident      *ast.Ident
			typeSpec   *ast.TypeSpec
			structType *ast.StructType
			arrayType  *ast.ArrayType

			flags GenFlag
		}
		var (
			depth int
			reset int
			items []*genItem
			item  *genItem

			astLog = func(s string, args ...interface{}) {
				log.Print(strings.Repeat(" ", depth*4), fmt.Sprintf(s, args...))
			}
		)
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
			case *ast.Ident:
				astLog("ident %q", v.Name)
				if item != nil {
					item.ident = v
				}
				return false

			case *ast.CommentGroup:
				for _, c := range v.List {
					astLog("comment %q", c.Text)

					text := strings.TrimPrefix(c.Text, "//ydb:")
					if c.Text != text {
						// Prepare item for furhter processing.
						astLog("probably process next item")
						flags, err := ParseComment(text)
						if err != nil {
							log.Fatalf(
								"malformed comment string: %q: %v",
								text, err,
							)
						}
						item = &genItem{
							flags: flags,
						}
						return false
					}
				}

			case *ast.TypeSpec:
				if item == nil {
					astLog("skipping type spec %q", v.Name)
					return false
				}
				item.typeSpec = v
				reset = depth
				astLog("processing type spec %q", v.Name)

			case *ast.StructType:
				astLog("struct %+v", v)
				item.structType = v
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
				item.arrayType = v
				items = append(items, item)
				item = nil
				reset = -1
				return true
			}

			return true
		})

		file := &File{
			Name: files[i].Name(),
		}
		for _, item := range items {
			switch {
			case item.arrayType != nil:
				s := &Slice{
					Name: item.ident.Name,
				}
				id, ok := item.arrayType.Elt.(*ast.Ident)
				if !ok {
					log.Fatalf("only type ident is expected as element of a slice")
				}
				pkg.Couple(id.Name, func(x interface{}) {
					s.Struct = x.(*Struct)
				})

				file.Slices = append(file.Slices, s)

			case item.structType != nil:
				s := &Struct{
					Name:  item.ident.Name,
					Flags: item.flags,
				}

				pkg.Register(item.ident.Name, s)
				decl := info.TypeOf(item.structType).(*types.Struct)

				for i, f := range item.structType.Fields.List {
					var err error
					field := &Field{
						Name: f.Names[0].Name,
						Type: info.TypeOf(f.Type),
					}
					if err := field.ParseTags(decl.Tag(i)); err != nil {
						log.Fatal(err)
					}
					if field.Ignore {
						continue
					}
					if field.Column != "" {
						s.SeekMode |= SeekName
					} else {
						field.Column = camelToSnake(field.Name)
					}
					if field.Position > 0 {
						s.SeekMode |= SeekPosition
					}
					if field.Primitive == 0 {
						field.Primitive, err = ydbtypes.PrimitiveTypeFromGoType(field.Type)
						if err != nil {
							log.Fatal(err)
						}
						field.BaseType = ydbtypes.GoTypeFromPrimitiveType(field.Primitive)
					}
					if err := field.Validate(); err != nil {
						log.Fatalf("generate struct %q error: %v", s.Name, err)
					}

					s.Fields = append(s.Fields, field)
				}
				if bits.OnesCount(uint(s.SeekMode)) > 1 {
					log.Fatal("ambiguous fields configuration: got either column names and position indexes")
				}
				if s.SeekMode == SeekPosition {
					sort.Slice(s.Fields, func(i, j int) bool {
						return s.Fields[i].Position < s.Fields[j].Position
					})
				}
				if s.SeekMode == SeekUnknown {
					s.SeekMode = SeekPosition
				}

				file.Structs = append(file.Structs, s)
			}
		}

		pkg.Files = append(pkg.Files, file)
	}

	if err := pkg.Finalize(); err != nil {
		log.Fatal(err)
	}

	g := Generator{}
	if err := g.Generate(pkg); err != nil {
		log.Fatal(err)
	}
}

type SeekMode uint

const (
	SeekUnknown SeekMode = 1 << iota >> 1
	SeekPosition
	SeekName
)

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
			"dependency not met: %d container type(s) want type %s to be generated",
			list.Len(), id,
		)
	}
	return nil
}

type GenFlag uint

const (
	GenNothing GenFlag = 1 << iota >> 1
	GenScan
	GenQueryParams
	GenContainer
)

func ParseComment(text string) (flags GenFlag, err error) {
	for _, param := range strings.Split(text, ",") {
		switch param {
		case "scan":
			flags |= GenScan
		case "params":
			flags |= GenQueryParams
		case "container":
			flags |= GenContainer
		default:
			return 0, fmt.Errorf("unknown parameter: %q", param)
		}
	}
	return flags, nil
}

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
	Name   string
	Struct *Struct
}

type Field struct {
	Name     string
	Column   string
	Position int
	Ignore   bool

	Conv      ConvMode
	Type      types.Type
	BaseType  types.Type // Type within database.
	Primitive internal.PrimitiveType
}

type ConvMode uint

const (
	ConvDefault = 1 << iota >> 1
	ConvUnsafe
	ConvAssert
)

func ParseConvMode(s string) (ConvMode, error) {
	switch s {
	case "unsafe":
		return ConvUnsafe, nil
	case "assert":
		return ConvAssert, nil
	default:
		return 0, fmt.Errorf("unknown conv mode: %q", s)
	}
}

func (f *Field) Validate() error {
	if !types.ConvertibleTo(f.Type, f.BaseType) {
		return fmt.Errorf(
			"impossible conversion %q type %s to %s (ydb type %s)",
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
	pairs := strings.Split(value, ",")
	var (
		columnGiven   bool
		positionGiven bool
	)
	for _, pair := range pairs {
		key, value := splitPair(pair)
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

func splitPair(p string) (key, value string) {
	i := strings.IndexByte(p, ':')
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
