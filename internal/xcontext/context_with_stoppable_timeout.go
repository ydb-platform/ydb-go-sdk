package xcontext

import (
	"context"
	"time"
)

// WithStoppableTimeout returns a copy of the parent context that is cancelled after
// timeout elapses, and a stop function. Calling the stop function prevents the
// timeout from cancelling the context and releases resources associated with it.
func WithStoppableTimeout(ctx context.Context, timeout time.Duration) (context.Context, func()) {
	ctxWithCancel, cancel := WithCancel(ctx)
	timeoutCtx, cancelTimeout := WithTimeout(ctx, timeout)

	stop := context.AfterFunc(timeoutCtx, cancel)

	return ctxWithCancel, func() {
		stop()
		cancelTimeout()
	}
}
