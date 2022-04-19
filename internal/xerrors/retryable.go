package xerrors

import (
	"errors"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
)

type retryableError struct {
	name              string
	err               error
	backoffType       backoff.Type
	mustDeleteSession bool
}

func (e *retryableError) Code() int32 {
	return -1
}

func (e *retryableError) Name() string {
	if e.name != "" {
		return e.name
	}
	return "CUSTOM"
}

func (e *retryableError) OperationStatus() operation.Status {
	return operation.NotFinished
}

func (e *retryableError) BackoffType() backoff.Type {
	return e.backoffType
}

func (e *retryableError) MustDeleteSession() bool {
	return e.mustDeleteSession
}

func (e *retryableError) Error() string {
	return e.err.Error()
}

type RetryableErrorOption func(e *retryableError)

func WithBackoff(t backoff.Type) RetryableErrorOption {
	return func(e *retryableError) {
		e.backoffType = t
	}
}

func WithName(name string) RetryableErrorOption {
	return func(e *retryableError) {
		e.name = name
	}
}

func WithDeleteSession() RetryableErrorOption {
	return func(e *retryableError) {
		e.mustDeleteSession = true
	}
}

func Retryable(err error, opts ...RetryableErrorOption) error {
	re := &retryableError{
		err: err,
	}
	for _, o := range opts {
		o(re)
	}
	return re
}

func RetryableError(err error) Error {
	var e *retryableError
	if errors.As(err, &e) {
		return e
	}
	return nil
}
