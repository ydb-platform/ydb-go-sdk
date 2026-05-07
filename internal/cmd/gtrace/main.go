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
	"sort"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var batch = flag.Bool("batch", false, "regenerate all *_gtrace.go files in the package directory (single typecheck)")

//nolint:gocyclo,funlen
func main() {
	flag.Parse()

	if *batch {
		args := flag.Args()
		if len(args) != 1 {
			log.Fatal("batch mode: expected exactly one argument (package directory path)")
		}
		dir, err := filepath.Abs(args[0])
		if err != nil {
			log.Fatal(err)
		}
		log.SetPrefix("[" + filepath.Base(dir) + " -batch] ")
		if err := runBatch(dir); err != nil {
			log.Fatal(err)
		}
		log.Println("OK")

		return
	}

	var (
		isGoGenerate bool
		gofile       string
		workDir      string
		err          error
	)
	if gofile = os.Getenv("GOFILE"); gofile != "" {
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

	srcFilePath := filepath.Join(workDir, gofile)

	fset, bctx, _, paths, astFiles, err := loadTracePackageAST(workDir)
	if err != nil {
		log.Fatal(err)
	}

	var writers []*Writer
	if isGoGenerate {
		openFile := func(name string) (*os.File, func()) {
			var f *os.File
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
		baseName := strings.TrimSuffix(gofile, ext)
		f, clean := openFile(baseName + "_gtrace" + ext)
		defer clean()
		writers = append(writers, &Writer{
			Context: bctx,
			Output:  f,
		})
	} else {
		writers = append(writers, &Writer{
			Context: bctx,
			Output:  os.Stdout,
		})
	}

	buildConstraints, err := buildConstraintsForFile(srcFilePath)
	if err != nil {
		panic(err)
	}

	items := extractGenItemsForFile(astFiles, paths, srcFilePath)

	info, pkg, err := typecheckPackage(fset, astFiles)
	if err != nil {
		panic(fmt.Sprintf("type error: %v", err))
	}

	p := buildPackage(pkg, info, buildConstraints, items)

	for _, w := range writers {
		if err := w.Write(p); err != nil {
			panic(err)
		}
	}

	log.Println("OK")
}

func runBatch(workDir string) error {
	fset, bctx, _, paths, astFiles, err := loadTracePackageAST(workDir)
	if err != nil {
		return err
	}
	info, pkg, err := typecheckPackage(fset, astFiles)
	if err != nil {
		return fmt.Errorf("type error: %w", err)
	}

	type outRec struct {
		path    string
		items   []*GenItem
		constrs []string
	}
	var outs []outRec
	for i, astFile := range astFiles {
		srcPath := paths[i]
		items := extractGenItems(astFile)
		if len(items) == 0 {
			continue
		}
		constrs, err := buildConstraintsForFile(srcPath)
		if err != nil {
			return err
		}
		outs = append(outs, outRec{
			path:    srcPath,
			items:   items,
			constrs: constrs,
		})
	}
	sort.Slice(outs, func(i, j int) bool {
		return outs[i].path < outs[j].path
	})

	for _, o := range outs {
		base := strings.TrimSuffix(filepath.Base(o.path), filepath.Ext(o.path))
		outPath := filepath.Join(workDir, base+"_gtrace.go")
		f, err := os.OpenFile(outPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600) //nolint:gosec,mnd
		if err != nil {
			return err
		}
		w := &Writer{
			Context: bctx,
			Output:  f,
		}
		p := buildPackage(pkg, info, o.constrs, o.items)
		if err := w.Write(p); err != nil {
			f.Close()

			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
	}

	return nil
}

func loadTracePackageAST(workDir string) (
	fset *token.FileSet,
	bctx build.Context,
	bpkg *build.Package,
	paths []string,
	astFiles []*ast.File,
	err error,
) {
	bctx = build.Default
	bpkg, err = bctx.ImportDir(workDir, build.IgnoreVendor)
	if err != nil {
		return nil, build.Context{}, nil, nil, nil, err
	}
	fset = token.NewFileSet()
	for _, name := range bpkg.GoFiles {
		base := strings.TrimSuffix(name, filepath.Ext(name))
		if isGenerated(base, "_gtrace") {
			continue
		}
		path := filepath.Join(workDir, name)
		var astFile *ast.File
		astFile, err = parser.ParseFile(fset, path, nil, parser.ParseComments)
		if err != nil {
			return nil, build.Context{}, nil, nil, nil, fmt.Errorf("parse %q: %w", path, err)
		}
		paths = append(paths, path)
		astFiles = append(astFiles, astFile)
	}

	return fset, bctx, bpkg, paths, astFiles, nil
}

func typecheckPackage(fset *token.FileSet, astFiles []*ast.File) (*types.Info, *types.Package, error) {
	info := &types.Info{
		Types: make(map[ast.Expr]types.TypeAndValue),
		Defs:  make(map[*ast.Ident]types.Object),
		Uses:  make(map[*ast.Ident]types.Object),
	}
	conf := types.Config{
		IgnoreFuncBodies:         true,
		DisableUnusedImportCheck: true,
		Importer:                 importer.ForCompiler(fset, "gc", nil),
	}
	pkg, err := conf.Check(".", fset, astFiles, info)
	if err == nil {
		return info, pkg, nil
	}

	info = &types.Info{
		Types: make(map[ast.Expr]types.TypeAndValue),
		Defs:  make(map[*ast.Ident]types.Object),
		Uses:  make(map[*ast.Ident]types.Object),
	}
	conf = types.Config{
		IgnoreFuncBodies:         true,
		DisableUnusedImportCheck: true,
		Importer:                 importer.ForCompiler(fset, "source", nil),
	}
	pkg, err2 := conf.Check(".", fset, astFiles, info)
	if err2 != nil {
		return nil, nil, fmt.Errorf("gc importer: %v; source importer: %w", err, err2)
	}

	return info, pkg, nil
}

func buildConstraintsForFile(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return scanBuildConstraints(f)
}

func extractGenItemsForFile(astFiles []*ast.File, paths []string, wantPath string) []*GenItem {
	for i := range astFiles {
		if paths[i] == wantPath {
			return extractGenItems(astFiles[i])
		}
	}

	panic(fmt.Sprintf("gtrace: source %q was not parsed", wantPath))
}

func extractGenItems(astFile *ast.File) []*GenItem {
	var items []*GenItem
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

	return items
}

func buildPackage(pkg *types.Package, info *types.Info, buildConstraints []string, items []*GenItem) Package {
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

	return p
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
