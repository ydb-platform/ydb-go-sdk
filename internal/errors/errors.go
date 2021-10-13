package errors

import (
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
