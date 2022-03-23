package errors

import (
	"context"
	"errors"
	"fmt"
	"io"

	grpcCodes "google.golang.org/grpc/codes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

type Error interface {
	error

	Code() int32
	Name() string
}

func IsTimeoutError(err error) bool {
	switch {
	case
		IsOperationError(
			err,
			Ydb.StatusIds_TIMEOUT,
			Ydb.StatusIds_CANCELLED,
		),
		IsTransportError(err, grpcCodes.Canceled, grpcCodes.DeadlineExceeded),
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
	if err == nil {
		return nil
	}
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
