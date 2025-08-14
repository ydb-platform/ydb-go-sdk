package utils

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
)

type FunctionIDArg struct {
	FuncDecl      *ast.FuncDecl
	ArgPos        token.Pos
	ArgEnd        token.Pos
	StackCallPath string
}

func ReadFile(filename string, info fs.FileInfo) ([]byte, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	defer f.Close()

	fileSize := int(info.Size())
	src := make([]byte, fileSize)
	n, err := io.ReadFull(f, src)
	if err != nil {
		return nil, err
	}

	if n < fileSize {
		return nil, fmt.Errorf("error: fileSize of %s changed during reading (from %d to %d bytes)", filename,
			fileSize, n)
	} else if n > fileSize {
		return nil, fmt.Errorf("error: fileSize of %s changed during reading (from %d to >=%d bytes)", filename,
			fileSize, len(src))
	}

	return src, nil
}

func FixSource(fset *token.FileSet, path string, src []byte, listOfArgs []FunctionIDArg) ([]byte, error) {
	var previousArgEnd int
	var fixedSource []byte

	for _, arg := range listOfArgs {
		argPosOffset := fset.Position(arg.ArgPos).Offset
		argEndOffset := fset.Position(arg.ArgEnd).Offset

		argument, err := makeCall(fset, path, arg)
		if err != nil {
			return nil, fmt.Errorf("error during making call path: %w", err)
		}

		fixedSource = append(fixedSource, src[previousArgEnd:argPosOffset]...)
		fixedSource = append(fixedSource, fmt.Sprintf("%q", argument)...)
		previousArgEnd = argEndOffset
	}
	fixedSource = append(fixedSource, src[previousArgEnd:]...)

	return fixedSource, nil
}

func WriteFile(filename string, formatted []byte, perm fs.FileMode) error {
	fout, err := os.OpenFile(filename, os.O_WRONLY|os.O_TRUNC, perm)
	if err != nil {
		return err
	}

	defer fout.Close()

	_, err = fout.Write(formatted)
	if err != nil {
		return err
	}

	return nil
}

func makeCall(fset *token.FileSet, path string, arg FunctionIDArg) (string, error) {
	funcName, err := getFuncName(arg.FuncDecl)
	if err != nil {
		return "", fmt.Errorf("error during getting function name: %w", err)
	}

	if arg.StackCallPath != "" {
		return arg.StackCallPath + "." + funcName, nil
	}

	modulePath := filepath.Join("github.com", "ydb-platform", version.Package, "v"+version.Major, "")
	packageName, err := getPackageName(fset, arg)
	if err != nil {
		return "", fmt.Errorf("error during getting package name for %s: %w", funcName, err)
	}

	filePath := filepath.Dir(filepath.Dir(path))

	return filepath.Join(modulePath, filePath, packageName) + "." + funcName, nil
}

func getFuncName(funcDecl *ast.FuncDecl) (string, error) {
	if funcDecl.Recv != nil {
		recvType := funcDecl.Recv.List[0].Type
		prefix, err := getIdentNameFromExpr(recvType)
		if err != nil {
			return "", err
		}

		return prefix + "." + funcDecl.Name.Name, nil
	}

	return funcDecl.Name.Name, nil
}

func getIdentNameFromExpr(expr ast.Expr) (string, error) {
	switch expr := expr.(type) {
	case *ast.Ident:
		return expr.Name, nil
	case *ast.StarExpr:
		prefix, err := getIdentNameFromExpr(expr.X)
		if err != nil {
			return "", err
		}

		return "(*" + prefix + ")", nil
	case *ast.IndexExpr:
		return getIdentNameFromExpr(expr.X)
	case *ast.IndexListExpr:
		return getIdentNameFromExpr(expr.X)
	default:
		return "", fmt.Errorf("error during getting ident from expr")
	}
}

func getPackageName(fset *token.FileSet, arg FunctionIDArg) (string, error) {
	file := fset.File(arg.ArgPos)
	parsedFile, err := parser.ParseFile(fset, file.Name(), nil, parser.PackageClauseOnly)
	if err != nil {
		return "", fmt.Errorf("error during getting package name function")
	}

	return parsedFile.Name.Name, nil
}
