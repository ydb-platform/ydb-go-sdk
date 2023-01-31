package retry

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type retryableErrorOption xerrors.RetryableErrorOption

const (
	TypeNoBackoff   = backoff.TypeNoBackoff
	TypeFastBackoff = backoff.TypeFast
	TypeSlowBackoff = backoff.TypeSlow
)

// WithBackoff makes retryable error option with custom backoff type
func WithBackoff(t backoff.Type) retryableErrorOption {
	return retryableErrorOption(xerrors.WithBackoff(t))
}

// WithDeleteSession makes retryable error option with delete session flag
func WithDeleteSession() retryableErrorOption {
	return retryableErrorOption(xerrors.WithDeleteSession())
}

// RetryableError makes retryable error from options
// RetryableError provides retrying on custom errors
func RetryableError(err error, opts ...retryableErrorOption) error {
	return xerrors.Retryable(
		err,
		func() (retryableErrorOptions []xerrors.RetryableErrorOption) {
			for _, o := range opts {
				if o != nil {
					retryableErrorOptions = append(retryableErrorOptions, xerrors.RetryableErrorOption(o))
				}
			}
			return retryableErrorOptions
		}()...,
	)
}
