package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	var (
		verbose bool
		suffix  string
		write   bool
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
	flag.Parse()

	log.SetFlags(log.Lshortfile)

	var (
		goGen   bool
		gofile  string
		workDir string
		err     error
	)
	if gofile = os.Getenv("GOFILE"); gofile != "" {
		goGen = true
		workDir, err = os.Getwd()
		if err != nil {
			log.Fatal(err)
		}
	} else {
		args := flag.Args()
		if len(args) == 0 {
			log.Fatal("no $GOFILE env nor file parameter are given")
		}
		gofile = filepath.Base(args[0])
		workDir = filepath.Dir(args[0])
	}

	srcFilePath := filepath.Join(workDir, gofile)
	pkgFilePaths, err := filepath.Glob(filepath.Join(workDir, "*.go"))
	if err != nil {
		log.Fatal(err)
	}
	if verbose {
		log.Printf("source file: %s", srcFilePath)
		log.Printf("package files: %v", pkgFilePaths)
	}

	var dest io.Writer
	if goGen || write {
		var (
			base = filepath.Base(srcFilePath)
			ext  = filepath.Ext(base)
			name = strings.TrimSuffix(base, ext)

			dstFilePath = filepath.Join(workDir, name+suffix+ext)
		)
		dstFile, err := os.OpenFile(dstFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			log.Fatal(err)
		}
		defer dstFile.Close()
		dest = dstFile
	} else {
		dest = os.Stdout
	}

	var (
		pkgFiles = make([]*os.File, 0, len(pkgFilePaths))
		astFiles = make([]*ast.File, 0, len(pkgFilePaths))
	)
	fset := token.NewFileSet()
	for _, p := range pkgFilePaths {
		if strings.HasSuffix(p, suffix+".go") {
			continue
		}
		if strings.HasSuffix(p, "_test.go") {
			continue
		}
		file, err := os.Open(p)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		ast, err := parser.ParseFile(fset, file.Name(), file, parser.ParseComments)
		if err != nil {
			log.Fatalf("parse %q error: %v", file.Name(), err)
		}

		pkgFiles = append(pkgFiles, file)
		astFiles = append(astFiles, ast)
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
				for _, c := range v.List {
					logf("comment %q", c.Text)

					text := strings.TrimPrefix(c.Text, "//gtrace:")
					if c.Text != text {
						if item == nil {
							item = &GenItem{
								File: pkgFiles[i],
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
	w := Writer{
		Output: dest,
	}
	p := Package{
		Package: pkg,
	}
	for _, item := range items {
		t := Trace{
			Name: item.Ident.Name,
			Flag: item.Flag,
		}
		for _, field := range item.StructType.Fields.List {
			name := field.Names[0].Name
			if fn, ok := field.Type.(*ast.FuncType); ok {
				f, err := buildFunc(info, fn)
				if err != nil {
					log.Printf(
						"skipping hook %s due to error: %v",
						name, err,
					)
					continue
				}
				t.Hooks = append(t.Hooks, Hook{
					Name: name,
					Func: f,
				})
			}
		}
		p.Traces = append(p.Traces, t)
	}
	if err := w.Write(p); err != nil {
		log.Fatal(err)
	}

	log.Println("OK")
}

func buildFunc(info types.Info, fn *ast.FuncType) (ret Func, err error) {
	for _, p := range fn.Params.List {
		t := info.TypeOf(p.Type)
		if t == nil {
			log.Fatalf("unknown type: %s", p.Type)
		}
		ret.Params = append(ret.Params, t)
	}
	if fn.Results == nil {
		return ret, nil
	}
	if len(fn.Results.List) > 1 {
		return ret, fmt.Errorf(
			"unsupported number of function results",
		)
	}

	p := fn.Results.List[0]
	fn, ok := p.Type.(*ast.FuncType)
	if !ok {
		return ret, fmt.Errorf(
			"unsupported function result type %s",
			info.TypeOf(p.Type),
		)
	}
	result, err := buildFunc(info, fn)
	if err != nil {
		return ret, err
	}
	ret.Result = append(ret.Result, result)

	return ret, nil
}

type Package struct {
	*types.Package
	Traces []Trace
}

type Trace struct {
	Name  string
	Hooks []Hook
	Flag  GenFlag
}

type Hook struct {
	Name string
	Func Func
}

type Func struct {
	Params []types.Type
	Result []Func // 0 or 1.
}

func (f Func) HasResult() bool {
	return len(f.Result) > 0
}

type GenFlag uint8

func (f GenFlag) Has(x GenFlag) bool {
	return f&x != 0
}

const (
	GenZero GenFlag = 1 << iota >> 1
	GenShortcut
	GenContext

	GenAll = ^GenFlag(0)
)

type GenItem struct {
	File       *os.File
	Ident      *ast.Ident
	TypeSpec   *ast.TypeSpec
	StructType *ast.StructType

	Flag GenFlag
}

func (x *GenItem) ParseComment(text string) (err error) {
	prefix, text := split(text, ' ')
	switch prefix {
	case "gen":
	case "set":
		return x.ParseParameter(text)
	default:
		return fmt.Errorf("unknown prefix: %q", prefix)
	}
	return nil
}

func (x *GenItem) ParseParameter(text string) (err error) {
	text = strings.TrimSpace(text)
	param, _ := split(text, '=')
	if param == "" {
		return nil
	}
	switch param {
	case "shortcut":
		x.Flag |= GenShortcut
	case "context":
		x.Flag |= GenContext
	default:
		return fmt.Errorf("unexpected parameter: %q", param)
	}
	return nil
}

func split(s string, c byte) (s1, s2 string) {
	i := strings.IndexByte(s, c)
	if i == -1 {
		return s, ""
	}
	return s[:i], s[i+1:]
}
