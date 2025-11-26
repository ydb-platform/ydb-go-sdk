package xcontext

import (
	"context"
	"time"
)

// WithStoppableTimeoutCause returns a copy of the parent context that is cancelled with
// the specified cause after timeout elapses, and a stop function. Calling the stop function
// prevents the timeout from canceling the context and releases resources associated with it.
// The cause error will be used when the timeout triggers cancellation.
//
// The returned stop function returns a boolean value:
//   - true if the timeout was successfully stopped before it fired (context was not cancelled by timeout)
//   - false if the timeout already fired and the context was cancelled with the specified cause
func WithStoppableTimeoutCause(ctx context.Context, timeout time.Duration, cause error) (context.Context, func() bool) {
	ctxWithCancel, cancel := context.WithCancelCause(ctx)
	timeoutCtx, cancelTimeout := WithTimeout(ctx, timeout)

	stop := context.AfterFunc(timeoutCtx, func() { cancel(cause) })

	return ctxWithCancel, func() bool {
		defer cancelTimeout()

		return stop()
	}
}
