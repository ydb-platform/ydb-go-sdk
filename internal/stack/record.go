package stack

import (
	"fmt"
	"path"
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

type functionDetails struct {
	pkgPath    string
	pkgName    string
	structName string
	funcName   string
	lambdas    []string
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

var _ Caller = call{}

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
		if opt != nil {
			opt(&optionsHolder)
		}
	}

	name, file := extractName(c.function, c.file)
	fnDetails := parseFunctionName(name)

	return buildRecordString(optionsHolder, &fnDetails, file, c.line)
}

func extractName(function uintptr, file string) (name, fileName string) {
	name = runtime.FuncForPC(function).Name()
	_, fileName = path.Split(file)
	name = strings.ReplaceAll(name, "[...]", "")

	return name, fileName
}

func parseFunctionName(name string) functionDetails {
	var details functionDetails
	if i := strings.LastIndex(name, "/"); i > -1 {
		details.pkgPath, name = name[:i], name[i+1:]
	}
	split := strings.Split(name, ".")
	details.lambdas = make([]string, 0, len(split))
	for i := range split {
		elem := split[len(split)-i-1]
		if !strings.HasPrefix(elem, "func") {
			break
		}
		details.lambdas = append(details.lambdas, elem)
	}
	split = split[:len(split)-len(details.lambdas)]
	if len(split) > 0 {
		details.pkgName = split[0]
	}
	if len(split) > 1 {
		details.funcName = split[len(split)-1]
	}
	if len(split) > 2 { //nolint:gomnd
		details.structName = split[1]
	}

	return details
}

func buildRecordString(
	optionsHolder recordOptions,
	fnDetails *functionDetails,
	file string,
	line int,
) string {
	buffer := xstring.Buffer()
	defer buffer.Free()
	if optionsHolder.packagePath {
		buffer.WriteString(fnDetails.pkgPath)
	}
	if optionsHolder.packageName {
		if buffer.Len() > 0 {
			buffer.WriteByte('/')
		}
		buffer.WriteString(fnDetails.pkgName)
	}
	if optionsHolder.structName && len(fnDetails.structName) > 0 {
		if buffer.Len() > 0 {
			buffer.WriteByte('.')
		}
		buffer.WriteString(fnDetails.structName)
	}
	if optionsHolder.functionName {
		if buffer.Len() > 0 {
			buffer.WriteByte('.')
		}
		buffer.WriteString(fnDetails.funcName)
		if optionsHolder.lambdas {
			for i := range fnDetails.lambdas {
				buffer.WriteByte('.')
				buffer.WriteString(fnDetails.lambdas[len(fnDetails.lambdas)-i-1])
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
			fmt.Fprintf(buffer, "%d", line)
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
