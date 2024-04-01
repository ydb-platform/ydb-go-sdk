package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/cmd/gstack/utils"
)

func usage() {
	fmt.Fprintf(os.Stderr, "usage: gstack [path]\n")
	flag.PrintDefaults()
}

func getCallExpressionsFromExpr(expr ast.Expr) (listOfCalls []*ast.CallExpr) {
	switch expr := expr.(type) {
	case *ast.SelectorExpr:
		listOfCalls = getCallExpressionsFromExpr(expr.X)
	case *ast.IndexExpr:
		listOfCalls = getCallExpressionsFromExpr(expr.X)
	case *ast.StarExpr:
		listOfCalls = getCallExpressionsFromExpr(expr.X)
	case *ast.BinaryExpr:
		listOfCalls = getCallExpressionsFromExpr(expr.X)
		listOfCalls = append(listOfCalls, getCallExpressionsFromExpr(expr.Y)...)
	case *ast.CallExpr:
		listOfCalls = append(listOfCalls, expr)
		listOfCalls = append(listOfCalls, getCallExpressionsFromExpr(expr.Fun)...)
		for _, arg := range expr.Args {
			listOfCalls = append(listOfCalls, getCallExpressionsFromExpr(arg)...)
		}
	case *ast.CompositeLit:
		for _, elt := range expr.Elts {
			listOfCalls = append(listOfCalls, getCallExpressionsFromExpr(elt)...)
		}
	case *ast.UnaryExpr:
		listOfCalls = append(listOfCalls, getCallExpressionsFromExpr(expr.X)...)
	case *ast.KeyValueExpr:
		listOfCalls = append(listOfCalls, getCallExpressionsFromExpr(expr.Value)...)
	case *ast.FuncLit:
		listOfCalls = append(listOfCalls, getListOfCallExpressionsFromBlockStmt(expr.Body)...)
	}

	return listOfCalls
}

func getExprFromDeclStmt(statement *ast.DeclStmt) (listOfExpressions []ast.Expr) {
	decl, ok := statement.Decl.(*ast.GenDecl)
	if !ok {
		return listOfExpressions
	}
	for _, spec := range decl.Specs {
		if spec, ok := spec.(*ast.ValueSpec); ok {
			listOfExpressions = append(listOfExpressions, spec.Values...)
		}
	}

	return listOfExpressions
}

func getCallExpressionsFromStmt(statement ast.Stmt) (listOfCallExpressions []*ast.CallExpr) {
	var body *ast.BlockStmt
	var listOfExpressions []ast.Expr
	switch stmt := statement.(type) {
	case *ast.IfStmt:
		body = stmt.Body
	case *ast.SwitchStmt:
		body = stmt.Body
	case *ast.TypeSwitchStmt:
		body = stmt.Body
	case *ast.SelectStmt:
		body = stmt.Body
	case *ast.ForStmt:
		body = stmt.Body
	case *ast.GoStmt:
		if fun, ok := stmt.Call.Fun.(*ast.FuncLit); ok {
			listOfCallExpressions = append(listOfCallExpressions, getListOfCallExpressionsFromBlockStmt(fun.Body)...)
		} else {
			listOfCallExpressions = append(listOfCallExpressions, stmt.Call)
		}
	case *ast.RangeStmt:
		body = stmt.Body
	case *ast.DeclStmt:
		listOfExpressions = append(listOfExpressions, getExprFromDeclStmt(stmt)...)
		for _, expr := range listOfExpressions {
			listOfCallExpressions = append(listOfCallExpressions, getCallExpressionsFromExpr(expr)...)
		}
	case *ast.CommClause:
		stmts := stmt.Body
		for _, stmt := range stmts {
			listOfCallExpressions = append(listOfCallExpressions, getCallExpressionsFromStmt(stmt)...)
		}
	case *ast.ExprStmt:
		listOfCallExpressions = append(listOfCallExpressions, getCallExpressionsFromExpr(stmt.X)...)
	case *ast.AssignStmt:
		for _, rh := range stmt.Rhs {
			listOfCallExpressions = append(listOfCallExpressions, getCallExpressionsFromExpr(rh)...)
		}
	case *ast.ReturnStmt:
		for _, result := range stmt.Results {
			listOfCallExpressions = append(listOfCallExpressions, getCallExpressionsFromExpr(result)...)
		}
	}
	if body != nil {
		listOfCallExpressions = append(
			listOfCallExpressions,
			getListOfCallExpressionsFromBlockStmt(body)...,
		)
	}

	return listOfCallExpressions
}

func getListOfCallExpressionsFromBlockStmt(block *ast.BlockStmt) (listOfCallExpressions []*ast.CallExpr) {
	for _, statement := range block.List {
		listOfCallExpressions = append(listOfCallExpressions, getCallExpressionsFromStmt(statement)...)
	}

	return listOfCallExpressions
}

func format(src []byte, path string, fset *token.FileSet, file *ast.File) ([]byte, error) {
	var listOfArgs []utils.FunctionIDArg
	for _, f := range file.Decls {
		var listOfCalls []*ast.CallExpr
		fn, ok := f.(*ast.FuncDecl)
		if !ok {
			continue
		}
		listOfCalls = getListOfCallExpressionsFromBlockStmt(fn.Body)
		for _, call := range listOfCalls {
			if function, ok := call.Fun.(*ast.SelectorExpr); ok && function.Sel.Name == "FunctionID" {
				pack, ok := function.X.(*ast.Ident)
				if !ok {
					continue
				}
				if pack.Name == "stack" && len(call.Args) == 1 {
					listOfArgs = append(listOfArgs, utils.FunctionIDArg{
						FuncDecl: fn,
						ArgPos:   call.Args[0].Pos(),
						ArgEnd:   call.Args[0].End(),
					})
				}
			}
		}
	}
	if len(listOfArgs) != 0 {
		fixed, err := utils.FixSource(fset, path, src, listOfArgs)
		if err != nil {
			return nil, err
		}

		return fixed, nil
	}

	return src, nil
}

func processFile(src []byte, path string, fset *token.FileSet, file *ast.File, info os.FileInfo) error {
	formatted, err := format(src, path, fset, file)
	if err != nil {
		return err
	}
	if !bytes.Equal(src, formatted) {
		err = utils.WriteFile(path, formatted, info.Mode().Perm())
		if err != nil {
			return err
		}
	}

	return nil
}

func main() {
	flag.Usage = usage
	flag.Parse()
	args := flag.Args()

	if len(args) != 1 {
		flag.Usage()

		return
	}
	_, err := os.Stat(args[0])
	if err != nil {
		panic(err)
	}

	fileSystem := os.DirFS(args[0])

	err = fs.WalkDir(fileSystem, ".", func(path string, d fs.DirEntry, err error) error {
		fset := token.NewFileSet()
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if filepath.Ext(path) == ".go" {
			info, err := os.Stat(path)
			if err != nil {
				return err
			}
			src, err := utils.ReadFile(path, info)
			if err != nil {
				return err
			}
			file, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				return err
			}

			return processFile(src, path, fset, file, info)
		}

		return nil
	})
	if err != nil {
		panic(err)
	}
}
