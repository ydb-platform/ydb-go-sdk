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
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

//nolint:gocyclo,funlen
func main() {
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
	buildPkg, err := buildCtx.ImportDir(workDir, build.IgnoreVendor)
	if err != nil {
		log.Fatal(err)
	}

	srcFilePath := filepath.Join(workDir, gofile)

	var writers []*Writer
	if isGoGenerate {
		openFile := func(name string) (*os.File, func()) {
			var f *os.File
			//nolint:gofumpt
			//nolint:nolintlint
			//nolint:gosec
			f, err = os.OpenFile(
				filepath.Join(workDir, filepath.Clean(name)),
				os.O_WRONLY|os.O_CREATE|os.O_TRUNC,
				0o600, //nolint:mnd
			)
			if err != nil {
				log.Fatal(err)
			}

			return f, func() { f.Close() }
		}
		ext := filepath.Ext(gofile)
		name := strings.TrimSuffix(gofile, ext)
		f, clean := openFile(name + "_gtrace" + ext)
		defer clean()
		writers = append(writers, &Writer{
			Context: buildCtx,
			Output:  f,
		})
	} else {
		writers = append(writers, &Writer{
			Context: buildCtx,
			Output:  os.Stdout,
		})
	}

	var (
		pkgFiles = make([]*os.File, 0, len(buildPkg.GoFiles))
		astFiles = make([]*ast.File, 0, len(buildPkg.GoFiles))

		buildConstraints []string
	)
	fset := token.NewFileSet()
	for _, name := range buildPkg.GoFiles {
		base := strings.TrimSuffix(name, filepath.Ext(name))
		if isGenerated(base, "_gtrace") {
			continue
		}
		var file *os.File
		file, err = os.Open(filepath.Join(workDir, name))
		if err != nil {
			panic(err)
		}
		defer file.Close() //nolint:gocritic

		var ast *ast.File
		ast, err = parser.ParseFile(fset, file.Name(), file, parser.ParseComments)
		if err != nil {
			panic(fmt.Sprintf("parse %q error: %v", file.Name(), err))
		}

		pkgFiles = append(pkgFiles, file)
		astFiles = append(astFiles, ast)

		if name == gofile {
			if _, err = file.Seek(0, io.SeekStart); err != nil {
				panic(err)
			}
			buildConstraints, err = scanBuildConstraints(file)
			if err != nil {
				panic(err)
			}
		}
	}
	info := &types.Info{
		Types: make(map[ast.Expr]types.TypeAndValue),
		Defs:  make(map[*ast.Ident]types.Object),
		Uses:  make(map[*ast.Ident]types.Object),
	}
	conf := types.Config{
		IgnoreFuncBodies:         true,
		DisableUnusedImportCheck: true,
		Importer:                 importer.ForCompiler(fset, "source", nil),
	}
	pkg, err := conf.Check(".", fset, astFiles, info)
	if err != nil {
		panic(fmt.Sprintf("type error: %v", err))
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
		ast.Inspect(astFile, func(n ast.Node) (next bool) {
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
			case *ast.FuncDecl, *ast.ValueSpec:
				return false

			case *ast.Ident:
				if item != nil {
					item.Ident = v
				}

				return false

			case *ast.CommentGroup:
				for _, c := range v.List {
					if strings.Contains(strings.TrimPrefix(c.Text, "//"), "gtrace:gen") {
						if item == nil {
							item = &GenItem{}
						}
					}
				}

				return false

			case *ast.StructType:
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
			t.Hooks = append(t.Hooks, Hook{
				Name: name,
				Func: f,
			})
		}
	}
	for _, w := range writers {
		if err := w.Write(p); err != nil {
			panic(err)
		}
	}

	log.Println("OK")
}

func buildFunc(info *types.Info, traces map[string]*Trace, fn *ast.FuncType) (ret *Func, err error) {
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
			return nil, xerrors.WithStackTrace(err)
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

type GenItem struct {
	Ident      *ast.Ident
	StructType *ast.StructType
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
			return nil, xerrors.WithStackTrace(err)
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

func isGenerated(base, suffix string) bool {
	i := strings.Index(base, suffix)
	if i == -1 {
		return false
	}
	n := len(base)
	m := i + len(suffix)

	return m == n || base[m] == '_'
}
