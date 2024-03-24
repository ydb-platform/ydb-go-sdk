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
	FuncDecl *ast.FuncDecl
	ArgPos   token.Pos
	ArgEnd   token.Pos
}

func ReadFile(filename string, info fs.FileInfo) ([]byte, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	size := int(info.Size())
	src := make([]byte, size)
	n, err := io.ReadFull(f, src)
	if err != nil {
		return nil, err
	}
	if n < size {
		return nil, fmt.Errorf("error: size of %q changed during reading (from %d to %d bytes)", filename, size, n)
	} else if n > size {
		return nil, fmt.Errorf("error: size of %q changed during reading (from %d to >=%d bytes)", filename, size, len(src))
	}

	return src, nil
}

func FixSource(fset *token.FileSet, path string, src []byte, listOfArgs []FunctionIDArg) ([]byte, error) {
	var fixed []byte
	var previousArgEnd int
	for _, arg := range listOfArgs {
		argPosOffset := fset.Position(arg.ArgPos).Offset
		argEndOffset := fset.Position(arg.ArgEnd).Offset
		argument, err := makeCall(fset, path, arg)
		if err != nil {
			return nil, err
		}
		fixed = append(fixed, src[previousArgEnd:argPosOffset]...)
		fixed = append(fixed, fmt.Sprintf("%q", argument)...)
		previousArgEnd = argEndOffset
	}
	fixed = append(fixed, src[previousArgEnd:]...)

	return fixed, nil
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
	basePath := filepath.Join("github.com", "ydb-platform", version.Prefix, version.Major, "")
	packageName, err := getPackageName(fset, arg)
	if err != nil {
		return "", err
	}
	filePath := filepath.Dir(filepath.Dir(path))
	funcName, err := getFuncName(arg.FuncDecl)
	if err != nil {
		return "", err
	}

	return filepath.Join(basePath, filePath, packageName) + "." + funcName, nil
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
		return "", fmt.Errorf("error during get package name function")
	}

	return parsedFile.Name.Name, nil
}
