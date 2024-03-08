package utils

import (
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"io"
	"io/fs"
	"os"
)

type FunctionIDArg struct {
	ArgPos int
	ArgEnd int
}

func ReadFile(filename string, info fs.FileInfo) ([]byte, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
		}
	}(f)
	size := int(info.Size())
	src := make([]byte, size)
	n, err := io.ReadFull(f, src)
	if err != nil {
		return nil, err
	}
	if n < size {
		return nil, fmt.Errorf("error: size of %s changed during reading (from %d to %d bytes)", filename, size, n)
	} else if n > size {
		return nil, fmt.Errorf("error: size of %s changed during reading (from %d to >=%d bytes)", filename, size, len(src))
	}
	return src, nil
}

func FixSource(src []byte, listOfArgs []FunctionIDArg) ([]byte, error) {
	var fixed []byte
	var previousArgEnd int
	for _, args := range listOfArgs {
		argument := stack.Call(1)
		fixed = append(fixed, src[previousArgEnd:args.ArgPos]...)
		fixed = append(fixed, fmt.Sprintf("\"%s\"", argument)...)
		previousArgEnd = args.ArgEnd
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
