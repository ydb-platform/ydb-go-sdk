package xerrors

import (
	"errors"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
)

type retryableError struct {
	name               string
	err                error
	backoffType        backoff.Type
	isRetryObjectValid bool
	code               int32
}

func (e *retryableError) Code() int32 {
	return e.code
}

func (e *retryableError) Name() string {
	return "retryable/" + e.name
}

func (e *retryableError) Type() Type {
	return TypeRetryable
}

func (e *retryableError) BackoffType() backoff.Type {
	return e.backoffType
}

func (e *retryableError) IsRetryObjectValid() bool {
	return e.isRetryObjectValid
}

func (e *retryableError) Error() string {
	return e.err.Error()
}

func (e *retryableError) Unwrap() error {
	return e.err
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

func InvalidObject() RetryableErrorOption {
	return func(e *retryableError) {
		e.isRetryObjectValid = true
	}
}

func Retryable(err error, opts ...RetryableErrorOption) error {
	var (
		e  Error
		re = &retryableError{
			err:                err,
			name:               "CUSTOM",
			code:               -1,
			isRetryObjectValid: true,
		}
	)
	if As(err, &e) {
		re.backoffType = e.BackoffType()
		re.isRetryObjectValid = e.IsRetryObjectValid()
		re.code = e.Code()
		re.name = e.Name()
	}
	for _, opt := range opts {
		if opt != nil {
			opt(re)
		}
	}

	return re
}

// RetryableError return Error if err is retriable error, else nil
func RetryableError(err error) Error {
	var unretriableErr untertryableError
	if errors.Is(err, unretriableErr) {
		return nil
	}

	var e *retryableError
	if errors.As(err, &e) {
		return e
	}

	return nil
}

func Nonretryable(err error) untertryableError {
	return untertryableError{err}
}

type untertryableError struct {
	error
}

func Unwrap(e untertryableError) error {
	return e.error
}
