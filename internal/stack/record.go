package stack

import (
	"fmt"
	"runtime"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xstring"
)

type recordOptions struct {
	packagePath  bool
	packageName  bool
	structName   bool
	functionName bool
	fileName     bool
	line         bool
	lambdas      bool
}

type recordOption func(opts *recordOptions)

func PackageName(b bool) recordOption {
	return func(opts *recordOptions) {
		opts.packageName = b
	}
}

func FunctionName(b bool) recordOption {
	return func(opts *recordOptions) {
		opts.functionName = b
	}
}

func FileName(b bool) recordOption {
	return func(opts *recordOptions) {
		opts.fileName = b
	}
}

func Line(b bool) recordOption {
	return func(opts *recordOptions) {
		opts.line = b
	}
}

func StructName(b bool) recordOption {
	return func(opts *recordOptions) {
		opts.structName = b
	}
}

func Lambda(b bool) recordOption {
	return func(opts *recordOptions) {
		opts.lambdas = b
	}
}

func PackagePath(b bool) recordOption {
	return func(opts *recordOptions) {
		opts.packagePath = b
	}
}

var _ caller = call{}

type call struct {
	function uintptr
	file     string
	line     int
}

func Call(depth int) (c call) {
	c.function, c.file, c.line, _ = runtime.Caller(depth + 1)
	return c
}

func (c call) Record(opts ...recordOption) string {
	optionsHolder := recordOptions{
		packagePath:  true,
		packageName:  true,
		structName:   true,
		functionName: true,
		fileName:     true,
		line:         true,
		lambdas:      true,
	}
	for _, opt := range opts {
		opt(&optionsHolder)
	}
	name := runtime.FuncForPC(c.function).Name()
	var (
		pkgPath    string
		pkgName    string
		structName string
		funcName   string
		file       = c.file
	)
	if i := strings.LastIndex(file, "/"); i > -1 {
		file = file[i+1:]
	}
	if i := strings.LastIndex(name, "/"); i > -1 {
		pkgPath, name = name[:i], name[i+1:]
	}
	split := strings.Split(name, ".")
	lambdas := make([]string, 0, len(split))
	for i := range split {
		elem := split[len(split)-i-1]
		if !strings.HasPrefix(elem, "func") {
			break
		}
		lambdas = append(lambdas, elem)
	}
	split = split[:len(split)-len(lambdas)]
	if len(split) > 0 {
		pkgName = split[0]
	}
	if len(split) > 1 {
		funcName = split[len(split)-1]
	}
	if len(split) > 2 {
		structName = split[1]
	}

	buffer := xstring.Buffer()
	defer buffer.Free()
	if optionsHolder.packagePath {
		buffer.WriteString(pkgPath)
	}
	if optionsHolder.packageName {
		if buffer.Len() > 0 {
			buffer.WriteByte('/')
		}
		buffer.WriteString(pkgName)
	}
	if optionsHolder.structName && len(structName) > 0 {
		if buffer.Len() > 0 {
			buffer.WriteByte('.')
		}
		buffer.WriteString(structName)
	}
	if optionsHolder.functionName {
		if buffer.Len() > 0 {
			buffer.WriteByte('.')
		}
		buffer.WriteString(funcName)
		if optionsHolder.lambdas {
			for i := range lambdas {
				buffer.WriteByte('.')
				buffer.WriteString(lambdas[len(lambdas)-i-1])
			}
		}
	}
	if optionsHolder.fileName {
		var closeBrace bool
		if buffer.Len() > 0 {
			buffer.WriteByte('(')
			closeBrace = true
		}
		buffer.WriteString(file)
		if optionsHolder.line {
			buffer.WriteByte(':')
			fmt.Fprintf(buffer, "%d", c.line)
		}
		if closeBrace {
			buffer.WriteByte(')')
		}
	}
	return buffer.String()
}

func (c call) FunctionID() string {
	return c.Record(Lambda(false), FileName(false))
}

func Record(depth int, opts ...recordOption) string {
	return Call(depth + 1).Record(opts...)
}
