package errors

import (
	"bytes"
	"context"
	"errors"
	"io"
)

func IsTimeoutError(err error) bool {
	switch {
	case
		IsOpError(err, StatusTimeout),
		IsOpError(err, StatusCancelled),
		IsTransportError(err, TransportErrorCanceled),
		IsTransportError(err, TransportErrorDeadlineExceeded),
		errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, context.Canceled):
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
	if err == io.EOF {
		return nil
	}
	return err
}

// As is a proxy to errors.As
// This need to single import errors
func As(err error, target interface{}) bool {
	if err == nil {
		return false
	}
	return errors.As(err, target)
}

// Is is a proxy to errors.Is
// This need to single import errors
func Is(err, target error) bool {
	return errors.Is(err, target)
}

// New is a proxy to errors.New
// This need to single import errors
func New(text string) error {
	return errors.New(text)
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
