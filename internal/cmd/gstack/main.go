package main

import (
	"flag"
	"fmt"
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
