package xerrors

import (
	"runtime"
	"strconv"
	"strings"
)

type withStackTraceOptions struct {
	skipDepth int
}

type withStackTraceOption func(o *withStackTraceOptions)

func WithSkipDepth(skipDepth int) withStackTraceOption {
	return func(o *withStackTraceOptions) {
		o.skipDepth = skipDepth
	}
}

// WithStackTrace is a wrapper over original err with file:line identification
func WithStackTrace(err error, opts ...withStackTraceOption) error {
	if err == nil {
		return nil
	}
	options := withStackTraceOptions{}
	for _, o := range opts {
		if o != nil {
			o(&options)
		}
	}
	return &stackError{
		stackRecord: StackRecord(options.skipDepth + 1),
		err:         err,
	}
}

func StackRecord(depth int) string {
	function, file, line, _ := runtime.Caller(depth + 1)
	name := runtime.FuncForPC(function).Name()
	return name + "(" + fileName(file) + ":" + strconv.Itoa(line) + ")"
}

func fileName(original string) string {
	i := strings.LastIndex(original, "/")
	if i == -1 {
		return original
	}
	return original[i+1:]
}

type stackError struct {
	stackRecord string
	err         error
}

func (e *stackError) Error() string {
	return e.err.Error() + " at `" + e.stackRecord + "`"
}

func (e *stackError) Unwrap() error {
	return e.err
}
