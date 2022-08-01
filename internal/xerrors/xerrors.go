package xerrors

import (
	"context"
	"errors"
	"io"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	grpcCodes "google.golang.org/grpc/codes"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
)

// Error is an interface of error which reports about error code and error name.
type Error interface {
	error

	Code() int32
	Name() string
	OperationStatus() operation.Status
	BackoffType() backoff.Type
	MustDeleteSession() bool
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
func As(err error, targets ...interface{}) (ok bool) {
	if err == nil {
		return false
	}
	for _, t := range targets {
		if errors.As(err, t) {
			if !ok {
				ok = true
			}
		}
	}
	return ok
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
