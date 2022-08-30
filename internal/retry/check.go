package retry

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

// Check returns retry mode for err.
func Check(err error) (
	statusCode int64,
	operationStatus operation.Status,
	backoffType backoff.Type,
	deleteSession bool,
) {
	var e xerrors.Error
	switch {
	case xerrors.As(err, &e):
		return int64(e.Code()), e.OperationStatus(), e.BackoffType(), e.MustDeleteSession()

	// context errors with explicit delete session
	case xerrors.Is(err, context.Canceled), xerrors.Is(err, context.DeadlineExceeded):
		return int64(-1), operation.Finished, backoff.TypeNoBackoff, true
	}

	return -1,
		operation.Finished, // it's finished, not need any retry attempts
		backoff.TypeNoBackoff,
		false
}

func MustDeleteSession(err error) bool {
	var e xerrors.Error
	switch {
	case xerrors.As(err, &e):
		return e.MustDeleteSession()

	// context errors with explicit delete session
	case xerrors.Is(err, context.Canceled), xerrors.Is(err, context.DeadlineExceeded):
		return true

	default:
		return false
	}
}

func MustRetry(err error, isOperationIdempotent bool) bool {
	var e xerrors.Error
	if xerrors.As(err, &e) {
		switch e.OperationStatus() {
		case operation.Finished:
			return false
		case operation.Undefined:
			return isOperationIdempotent
		default:
			return true
		}
	}
	return false
}
