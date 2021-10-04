package ydb

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
)

type StatusCode errors.StatusCode

const (
	StatusAlreadyExists = StatusCode(errors.StatusAlreadyExists)
)

func IsTimeoutError(err error) bool {
	return errors.IsTimeoutError(err)
}

func IsTransportError(err error) (ok bool, code int32, name string) {
	var t *errors.TransportError
	if !errors.As(err, &t) {
		return
	}
	return true, int32(t.Reason), t.Reason.String()
}

func IsOperationError(err error) (ok bool, code int32, name string) {
	var o *errors.OpError
	if !errors.As(err, &o) {
		return
	}
	return true, int32(o.Reason), o.Reason.String()
}

// IsOpError reports whether err is OpError with given code as the Reason.
func IsOpError(err error, code StatusCode) bool {
	var op *errors.OpError
	if !errors.As(err, &op) {
		return false
	}
	return op.Reason == errors.StatusCode(code)
}
