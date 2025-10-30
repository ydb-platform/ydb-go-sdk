package xcontext

import (
	"context"
	"time"
)

// WithStoppableTimeoutCause returns a copy of the parent context that is cancelled with
// the specified cause after timeout elapses, and a stop function. Calling the stop function
// prevents the timeout from canceling the context and releases resources associated with it.
// The cause error will be used when the timeout triggers cancellation.
func WithStoppableTimeoutCause(ctx context.Context, timeout time.Duration, cause error) (context.Context, func()) {
	ctxWithCancel, cancel := context.WithCancelCause(ctx)
	timeoutCtx, cancelTimeout := WithTimeout(ctx, timeout)

	stop := context.AfterFunc(timeoutCtx, func() { cancel(cause) })

	return ctxWithCancel, func() {
		stop()
		cancelTimeout()
	}
}
