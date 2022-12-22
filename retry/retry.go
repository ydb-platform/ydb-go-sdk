package retry

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/wait"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// retryOperation is the interface that holds an operation for retry.
// if retryOperation returns not nil - operation will retry
// if retryOperation returns nil - retry loop will break
type retryOperation func(context.Context) (err error)

type retryOptions struct {
	id          string
	trace       trace.Retry
	idempotent  bool
	stackTrace  bool
	fastBackoff backoff.Backoff
	slowBackoff backoff.Backoff

	panicCallback func(e interface{})
}

type retryOption func(o *retryOptions)

// WithID applies id for identification call Retry in trace.Retry.OnRetry
func WithID(id string) retryOption {
	return func(o *retryOptions) {
		o.id = id
	}
}

// WithStackTrace wraps errors with stacktrace from Retry call
func WithStackTrace() retryOption {
	return func(o *retryOptions) {
		o.stackTrace = true
	}
}

// WithTrace returns trace option
func WithTrace(trace trace.Retry) retryOption {
	return func(o *retryOptions) {
		o.trace = trace
	}
}

// WithIdempotent applies idempotent flag to retry operation
func WithIdempotent(idempotent bool) retryOption {
	return func(o *retryOptions) {
		o.idempotent = idempotent
	}
}

// WithFastBackoff replaces default fast backoff
func WithFastBackoff(b backoff.Backoff) retryOption {
	return func(o *retryOptions) {
		o.fastBackoff = b
	}
}

// WithSlowBackoff replaces default slow backoff
func WithSlowBackoff(b backoff.Backoff) retryOption {
	return func(o *retryOptions) {
		o.slowBackoff = b
	}
}

// WithPanicCallback returns panic callback option
// If not defined - panic would not intercept with driver
func WithPanicCallback(panicCallback func(e interface{})) retryOption {
	return func(o *retryOptions) {
		o.panicCallback = panicCallback
	}
}

type (
	markRetryCallKey struct{}
)

func markRetryCall(ctx context.Context) context.Context {
	return context.WithValue(ctx, markRetryCallKey{}, true)
}

func isRetryCalledAbove(ctx context.Context) bool {
	if _, has := ctx.Value(markRetryCallKey{}).(bool); has {
		return true
	}
	return false
}

// Retry provide the best effort fo retrying operation
//
// Retry implements internal busy loop until one of the following conditions is met:
//
// - context was canceled or deadlined
//
// - retry operation returned nil as error
//
// Warning: if deadline without deadline or cancellation func Retry will be worked infinite
//
// If you need to retry your op func on some logic errors - you must return RetryableError() from retryOperation
func Retry(ctx context.Context, op retryOperation, opts ...retryOption) (err error) {
	options := &retryOptions{
		fastBackoff: backoff.Fast,
		slowBackoff: backoff.Slow,
	}
	for _, o := range opts {
		o(options)
	}
	ctx = xcontext.WithIdempotent(ctx, options.idempotent)
	defer func() {
		if err != nil && options.stackTrace {
			err = xerrors.WithStackTrace(
				err,
				xerrors.WithSkipDepth(2), // 1 - exit from defer, 1 - exit from Retry call
			)
		}
	}()
	var (
		i        int
		attempts int

		code           = int64(0)
		onIntermediate = trace.RetryOnRetry(options.trace, &ctx, options.id, options.idempotent, isRetryCalledAbove(ctx))
	)
	defer func() {
		onIntermediate(err)(attempts, err)
	}()
	for {
		i++
		attempts++
		select {
		case <-ctx.Done():
			return xerrors.WithStackTrace(ctx.Err())

		default:
			err = func() error {
				if options.panicCallback != nil {
					defer func() {
						if e := recover(); e != nil {
							options.panicCallback(e)
						}
					}()
				}
				return op(markRetryCall(ctx))
			}()

			if err == nil {
				return
			}

			m := Check(err)

			if m.StatusCode() != code {
				i = 0
			}

			if !m.MustRetry(options.idempotent) {
				return xerrors.WithStackTrace(err)
			}

			if e := wait.Wait(ctx, options.fastBackoff, options.slowBackoff, m.BackoffType(), i); e != nil {
				return xerrors.WithStackTrace(err)
			}

			code = m.StatusCode()

			onIntermediate(err)
		}
	}
}

// Check returns retry mode for queryErr.
func Check(err error) (m retryMode) {
	code, errType, backoff, deleteSession := xerrors.Check(err)
	return retryMode{
		code:          code,
		errType:       errType,
		backoff:       backoff,
		deleteSession: deleteSession,
	}
}
