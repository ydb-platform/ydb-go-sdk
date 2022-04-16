package retry

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/wait"
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
	fastBackoff backoff.Backoff
	slowBackoff backoff.Backoff

	panicCallback func(e interface{})
}

type retryOption func(h *retryOptions)

// WithID applies id for identification call Retry in trace.Retry.OnRetry
func WithID(id string) retryOption {
	return func(h *retryOptions) {
		h.id = id
	}
}

// WithTrace returns trace option
func WithTrace(trace trace.Retry) retryOption {
	return func(h *retryOptions) {
		h.trace = trace
	}
}

// WithIdempotent applies idempotent flag to retry operation
func WithIdempotent(idempotent bool) retryOption {
	return func(h *retryOptions) {
		h.idempotent = idempotent
	}
}

// WithFastBackoff replaces default fast backoff
func WithFastBackoff(b backoff.Backoff) retryOption {
	return func(h *retryOptions) {
		h.fastBackoff = b
	}
}

// WithSlowBackoff replaces default slow backoff
func WithSlowBackoff(b backoff.Backoff) retryOption {
	return func(h *retryOptions) {
		h.slowBackoff = b
	}
}

// WithPanicCallback returns panic callback option
// If not defined - panic would not intercept with driver
func WithPanicCallback(panicCallback func(e interface{})) retryOption {
	return func(h *retryOptions) {
		h.panicCallback = panicCallback
	}
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
	var (
		i        int
		attempts int

		code           = int64(0)
		onIntermediate = trace.RetryOnRetry(options.trace, &ctx, options.id, options.idempotent)
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
				return op(ctx)
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

// Check returns retry mode for err.
func Check(err error) (m retry.Mode) {
	statusCode, operationStatus, backoff, deleteSession := retry.Check(err)
	return retry.NewMode(
		statusCode,
		operationStatus,
		backoff,
		deleteSession,
	)
}
