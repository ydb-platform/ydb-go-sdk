package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/build"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"text/tabwriter"

	_ "unsafe" // For go:linkname.

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
)

//go:linkname build_goodOSArchFile go/build.(*Context).goodOSArchFile
// nolint:revive
func build_goodOSArchFile(*build.Context, string, map[string]bool) bool

// nolint:gocyclo
func main() {
	var (
		verbose    bool
		suffix     string
		stubSuffix string
		write      bool
		buildTag   string
	)
	flag.BoolVar(&verbose,
		"v", false,
		"output debug info",
	)
	flag.BoolVar(&write,
		"w", false,
		"write trace to file",
	)
	flag.StringVar(&suffix,
		"file-suffix", "_gtrace",
		"suffix for generated go files",
	)
	flag.StringVar(&stubSuffix,
		"stub-file-suffix", "_stub",
		"suffix for generated stub go files",
	)
	flag.StringVar(&buildTag,
		"tag", "",
		"build tag which needs to be passed to enable tracing",
	)
	flag.Parse()

	if verbose {
		log.SetFlags(log.Lshortfile)
	} else {
		log.SetFlags(0)
	}

	var (
		// Reports whether we were called from go:generate.
		isGoGenerate bool

		gofile  string
		workDir string
		err     error
	)
	if gofile = os.Getenv("GOFILE"); gofile != "" {
		// NOTE: GOFILE is always a filename without path.
		isGoGenerate = true
		workDir, err = os.Getwd()
		if err != nil {
			log.Fatal(err)
		}
	} else {
		args := flag.Args()
		if len(args) == 0 {
			log.Fatal("no $GOFILE env nor file parameter were given")
		}
		gofile = filepath.Base(args[0])
		workDir = filepath.Dir(args[0])
	}
	{
		prefix := filepath.Join(filepath.Base(workDir), gofile)
		log.SetPrefix("[" + prefix + "] ")
	}
	buildCtx := build.Default
	if verbose {
		var sb strings.Builder
		prettyPrint(&sb, buildCtx)
		log.Printf("build context:\n%s", sb.String())
	}
	buildPkg, err := buildCtx.ImportDir(workDir, build.IgnoreVendor)
	if err != nil {
		log.Fatal(err)
	}

	srcFilePath := filepath.Join(workDir, gofile)
	if verbose {
		log.Printf("source file: %s", srcFilePath)
		log.Printf("package files: %v", buildPkg.GoFiles)
	}

	var writers []*Writer
	// nolint:nestif
	if isGoGenerate || write {
		// We should respect Go suffixes like `_linux.go`.
		name, tags, ext := splitOSArchTags(&buildCtx, gofile)
		if verbose {
			log.Printf(
				"split os/args tags of %q: %q %q %q",
				gofile, name, tags, ext,
			)
		}
		openFile := func(name string) (*os.File, func()) {
			p := filepath.Join(workDir, name)
			if verbose {
				log.Printf("destination file path: %+v", p)
			}
			var f *os.File
			f, err = os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
			if err != nil {
				log.Fatal(err)
			}
			return f, func() { f.Close() }
		}
		f, clean := openFile(name + suffix + tags + ext)
		defer clean()
		writers = append(writers, &Writer{
			Context:  buildCtx,
			Output:   f,
			BuildTag: buildTag,
		})
		if buildTag != "" {
			f, clean := openFile(name + suffix + stubSuffix + tags + ext)
			defer clean()
			writers = append(writers, &Writer{
				Context:  buildCtx,
				Output:   f,
				BuildTag: buildTag,
			})
		}
	} else {
		writers = append(writers, &Writer{
			Context:  buildCtx,
			Output:   os.Stdout,
			BuildTag: buildTag,
		})
	}

	var (
		pkgFiles = make([]*os.File, 0, len(buildPkg.GoFiles))
		astFiles = make([]*ast.File, 0, len(buildPkg.GoFiles))

		buildConstraints []string
	)
	fset := token.NewFileSet()
	for _, name := range buildPkg.GoFiles {
		base, _, _ := splitOSArchTags(&buildCtx, name)
		if isGenerated(base, suffix) {
			// Skip gtrace generated files.
			if verbose {
				log.Printf("skipped package file: %q", name)
			}
			continue
		}
		if verbose {
			log.Printf("parsing package file: %q", name)
		}
		var file *os.File
		file, err = os.Open(filepath.Join(workDir, name))
		if err != nil {
			// nolint:gocritic
			log.Fatal(err)
		}
		defer file.Close()

		var ast *ast.File
		ast, err = parser.ParseFile(fset, file.Name(), file, parser.ParseComments)
		if err != nil {
			log.Fatalf("parse %q error: %v", file.Name(), err)
		}

		pkgFiles = append(pkgFiles, file)
		astFiles = append(astFiles, ast)

		if name == gofile {
			if _, err = file.Seek(0, io.SeekStart); err != nil {
				log.Fatal(err)
			}
			buildConstraints, err = scanBuildConstraints(file)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
	info := types.Info{
		Types: make(map[ast.Expr]types.TypeAndValue),
		Defs:  make(map[*ast.Ident]types.Object),
		Uses:  make(map[*ast.Ident]types.Object),
	}
	conf := types.Config{
		IgnoreFuncBodies:         true,
		DisableUnusedImportCheck: true,
		Importer:                 importer.ForCompiler(fset, "source", nil),
	}
	pkg, err := conf.Check(".", fset, astFiles, &info)
	if err != nil {
		log.Fatalf("type error: %v", err)
	}
	var items []*GenItem
	for i, astFile := range astFiles {
		if pkgFiles[i].Name() != srcFilePath {
			continue
		}
		var (
			depth int
			item  *GenItem
		)
		logf := func(s string, args ...interface{}) {
			if !verbose {
				return
			}
			log.Print(
				strings.Repeat(" ", depth*4),
				fmt.Sprintf(s, args...),
			)
		}
		ast.Inspect(astFile, func(n ast.Node) (next bool) {
			logf("%T", n)

			if n == nil {
				item = nil
				depth--
				return true
			}
			defer func() {
				if next {
					depth++
				}
			}()

			switch v := n.(type) {
			case
				*ast.FuncDecl,
				*ast.ValueSpec:
				return false

			case *ast.Ident:
				logf("ident %q", v.Name)
				if item != nil {
					item.Ident = v
				}
				return false

			case *ast.CommentGroup:
				for i, c := range v.List {
					logf("#%d comment %q", i, c.Text)

					text, ok := TrimConfigComment(c.Text)
					if ok {
						if item == nil {
							item = &GenItem{}
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

			case *ast.StructType:
				logf("struct %+v", v)
				if item != nil {
					item.StructType = v
					items = append(items, item)
					item = nil
				}
				return false
			}

			return true
		})
	}
	p := Package{
		Package:          pkg,
		BuildConstraints: buildConstraints,
	}
	traces := make(map[string]*Trace)
	for _, item := range items {
		t := &Trace{
			Name: item.Ident.Name,
		}
		p.Traces = append(p.Traces, t)
		traces[item.Ident.Name] = t
	}
	for i, item := range items {
		t := p.Traces[i]
		for _, field := range item.StructType.Fields.List {
			if _, ok := field.Type.(*ast.FuncType); !ok {
				continue
			}
			name := field.Names[0].Name
			fn, ok := field.Type.(*ast.FuncType)
			if !ok {
				continue
			}
			f, err := buildFunc(info, traces, fn)
			if err != nil {
				log.Printf(
					"skipping hook %s due to error: %v",
					name, err,
				)
				continue
			}
			var config GenConfig
			if doc := field.Doc; doc != nil {
				for _, line := range doc.List {
					text, ok := TrimConfigComment(line.Text)
					if !ok {
						continue
					}
					err := config.ParseComment(text)
					if err != nil {
						log.Fatalf(
							"malformed comment string: %q: %v",
							text, err,
						)
					}
				}
			}
			t.Hooks = append(t.Hooks, Hook{
				Name: name,
				Func: f,
			})
		}
	}
	for _, w := range writers {
		if err := w.Write(p); err != nil {
			log.Fatal(err)
		}
	}

	log.Println("OK")
}

func buildFunc(info types.Info, traces map[string]*Trace, fn *ast.FuncType) (ret *Func, err error) {
	ret = new(Func)
	for _, p := range fn.Params.List {
		t := info.TypeOf(p.Type)
		if t == nil {
			log.Fatalf("unknown type: %s", p.Type)
		}
		var names []string
		for _, n := range p.Names {
			name := n.Name
			if name == "_" {
				name = ""
			}
			names = append(names, name)
		}
		if len(names) == 0 {
			// Case where arg is not named.
			names = []string{""}
		}
		for _, name := range names {
			ret.Params = append(ret.Params, Param{
				Name: name,
				Type: t,
			})
		}
	}
	if fn.Results == nil {
		return ret, nil
	}
	if len(fn.Results.List) > 1 {
		return nil, fmt.Errorf(
			"unsupported number of function results",
		)
	}

	r := fn.Results.List[0]

	switch x := r.Type.(type) {
	case *ast.FuncType:
		result, err := buildFunc(info, traces, x)
		if err != nil {
			return nil, errors.WithStackTrace(err)
		}
		ret.Result = append(ret.Result, result)
		return ret, nil

	case *ast.Ident:
		if t, ok := traces[x.Name]; ok {
			t.Nested = true
			ret.Result = append(ret.Result, t)
			return ret, nil
		}
	}

	return nil, fmt.Errorf(
		"unsupported function result type %s",
		info.TypeOf(r.Type),
	)
}

func splitOSArchTags(ctx *build.Context, name string) (base, tags, ext string) {
	fileTags := make(map[string]bool)
	build_goodOSArchFile(ctx, name, fileTags)
	ext = filepath.Ext(name)
	switch len(fileTags) {
	case 0: // *
		base = strings.TrimSuffix(name, ext)

	case 1: // *_GOOS or *_GOARCH
		i := strings.LastIndexByte(name, '_')

		base = name[:i]
		tags = strings.TrimSuffix(name[i:], ext)

	case 2: // *_GOOS_GOARCH
		var i int
		i = strings.LastIndexByte(name, '_')
		i = strings.LastIndexByte(name[:i], '_')

		base = name[:i]
		tags = strings.TrimSuffix(name[i:], ext)

	default:
		panic(fmt.Sprintf(
			"gtrace: internal error: unexpected number of OS/arch tags: %d",
			len(fileTags),
		))
	}
	return
}

type Package struct {
	*types.Package

	BuildConstraints []string
	Traces           []*Trace
}

type Trace struct {
	Name   string
	Hooks  []Hook
	Nested bool
}

func (*Trace) isFuncResult() bool { return true }

type Hook struct {
	Name string
	Func *Func
}

type Param struct {
	Name string // Might be empty.
	Type types.Type
}

func (p Param) String() string {
	return p.Name + " " + p.Type.String()
}

type FuncResult interface {
	isFuncResult() bool
}

type Func struct {
	Params []Param
	Result []FuncResult // 0 or 1.
}

func (*Func) isFuncResult() bool { return true }

func (f *Func) HasResult() bool {
	return len(f.Result) > 0
}

type GenFlag uint8

func (f GenFlag) Has(x GenFlag) bool {
	return f&x != 0
}

type GenConfig struct {
}

func TrimConfigComment(text string) (string, bool) {
	s := strings.TrimPrefix(text, "//gtrace:")
	if text != s {
		return s, true
	}
	return "", false
}

func (g *GenConfig) ParseComment(text string) (err error) {
	prefix, text := split(text, ' ')
	switch prefix {
	case "gen":
	default:
		return fmt.Errorf("unknown prefix: %q", prefix)
	}
	return nil
}

type GenItem struct {
	GenConfig
	Ident      *ast.Ident
	StructType *ast.StructType
}

func split(s string, c byte) (s1, s2 string) {
	i := strings.IndexByte(s, c)
	if i == -1 {
		return s, ""
	}
	return s[:i], s[i+1:]
}

func rsplit(s string, c byte) (s1, s2 string) {
	i := strings.LastIndexByte(s, c)
	if i == -1 {
		return s, ""
	}
	return s[:i], s[i+1:]
}

func scanBuildConstraints(r io.Reader) (cs []string, err error) {
	br := bufio.NewReader(r)
	for {
		line, err := br.ReadBytes('\n')
		if err != nil {
			return nil, errors.WithStackTrace(err)
		}
		line = bytes.TrimSpace(line)
		if comm := bytes.TrimPrefix(line, []byte("//")); !bytes.Equal(comm, line) {
			comm = bytes.TrimSpace(comm)
			if bytes.HasPrefix(comm, []byte("+build")) {
				cs = append(cs, string(line))
				continue
			}
		}
		if bytes.HasPrefix(line, []byte("package ")) {
			break
		}
	}
	return cs, nil
}

func prettyPrint(w io.Writer, x interface{}) {
	tw := tabwriter.NewWriter(w, 0, 2, 2, ' ', 0)
	t := reflect.TypeOf(x)
	v := reflect.ValueOf(x)
	for i := 0; i < t.NumField(); i++ {
		if v.Field(i).IsZero() {
			continue
		}
		fmt.Fprintf(tw, "%s:\t%v\n",
			t.Field(i).Name,
			v.Field(i),
		)
	}
	tw.Flush()
}

func isGenerated(base, suffix string) bool {
	i := strings.Index(base, suffix)
	if i == -1 {
		return false
	}
	n := len(base)
	m := i + len(suffix)
	return m == n || base[m] == '_'
}
