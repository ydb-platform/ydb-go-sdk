package xerrors

import (
	"errors"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
)

type retryableError struct {
	name              string
	err               error
	backoffType       backoff.Type
	mustDeleteSession bool
	code              int32
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

func (e *retryableError) MustDeleteSession() bool {
	return e.mustDeleteSession
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

func WithDeleteSession() RetryableErrorOption {
	return func(e *retryableError) {
		e.mustDeleteSession = true
	}
}

func Retryable(err error, opts ...RetryableErrorOption) error {
	var (
		e  Error
		re = &retryableError{
			err:  err,
			name: "CUSTOM",
			code: -1,
		}
	)
	if As(err, &e) {
		re.backoffType = e.BackoffType()
		re.mustDeleteSession = e.MustDeleteSession()
		re.code = e.Code()
		re.name = e.Name()
	}
	for _, o := range opts {
		if o != nil {
			o(re)
		}
	}
	return re
}

// RetryableError return Error if err is retriable error, else nil
func RetryableError(err error) Error {
	var e *retryableError
	if errors.As(err, &e) {
		return e
	}
	return nil
}
