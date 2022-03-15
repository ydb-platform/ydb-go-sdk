package errors

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strconv"
	"strings"
)

func IsTimeoutError(err error) bool {
	switch {
	case
		IsOpError(
			err,
			StatusTimeout,
			StatusCancelled,
		),
		IsTransportError(err, TransportErrorCanceled, TransportErrorDeadlineExceeded),
		Is(
			err,
			context.DeadlineExceeded,
			context.Canceled,
		):
		return true
	default:
		return false
	}
}

func ErrIf(cond bool, err error) error {
	if cond {
		return err
	}
	return nil
}

func HideEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

// As is a proxy to errors.As
// This need to single import errors
func As(err error, targets ...interface{}) bool {
	if err == nil {
		return false
	}
	for _, t := range targets {
		if errors.As(err, t) {
			return true
		}
	}
	return false
}

// Is is a improved proxy to errors.Is
// This need to single import errors
func Is(err error, targets ...error) bool {
	if len(targets) == 0 {
		panic("empty targets")
	}
	for _, target := range targets {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

// New is a proxy to errors.New
// This need to single import errors
func New(text string) error {
	return WithStackTrace(fmt.Errorf("%w", errors.New(text)), WithSkipDepth(1))
}

// NewWithIssues returns error which contains child issues
func NewWithIssues(text string, issues ...error) error {
	return &errorWithIssues{
		reason: text,
		issues: issues,
	}
}

type errorWithIssues struct {
	reason string
	issues []error
}

func (e *errorWithIssues) Error() string {
	var b bytes.Buffer
	b.WriteString(e.reason)
	b.WriteString(", issues: [")
	for i, issue := range e.issues {
		if i != 0 {
			b.WriteString(", ")
		}
		b.WriteString(issue.Error())
	}
	b.WriteString("]")
	return b.String()
}

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
	options := withStackTraceOptions{}
	for _, o := range opts {
		o(&options)
	}
	return &stackError{
		stackRecord: stackRecord(options.skipDepth + 1),
		err:         err,
	}
}

func stackRecord(depth int) string {
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

func TraceError(err error, noTraceErrors ...interface{}) error {
	if As(err, noTraceErrors...) {
		return nil
	}
	return err
}
