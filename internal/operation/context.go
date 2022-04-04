package operation

import (
	"context"
	"time"
)

type (
	ctxOperationTimeoutKey     struct{}
	ctxOperationCancelAfterKey struct{}
)

// WithTimeout returns a copy of parent context in which YDB operation timeout
// parameter is set to d. If parent context timeout is smaller than d, parent context is returned.
func WithTimeout(ctx context.Context, operationTimeout time.Duration) context.Context {
	if d, ok := Timeout(ctx); ok && operationTimeout >= d {
		// The current cancelation timeout is already smaller than the new one.
		return ctx
	}
	return context.WithValue(ctx, ctxOperationTimeoutKey{}, operationTimeout)
}

// WithCancelAfter returns a copy of parent context in which YDB operation
// cancel after parameter is set to d. If parent context cancellation timeout is smaller
// than d, parent context is returned.
func WithCancelAfter(ctx context.Context, operationCancelAfter time.Duration) context.Context {
	if d, ok := CancelAfter(ctx); ok && operationCancelAfter >= d {
		// The current cancelation timeout is already smaller than the new one.
		return ctx
	}
	return context.WithValue(ctx, ctxOperationCancelAfterKey{}, operationCancelAfter)
}

// Timeout returns the timeout within given context after which
// YDB should try to cancel operation and return result regardless of the cancelation.
func Timeout(ctx context.Context) (d time.Duration, ok bool) {
	d, ok = ctx.Value(ctxOperationTimeoutKey{}).(time.Duration)
	return
}

// CancelAfter returns the timeout within given context after which
// YDB should try to cancel operation and return result regardless of the cancellation.
func CancelAfter(ctx context.Context) (d time.Duration, ok bool) {
	d, ok = ctx.Value(ctxOperationCancelAfterKey{}).(time.Duration)
	return
}

func untilDeadline(ctx context.Context) (time.Duration, bool) {
	deadline, ok := ctx.Deadline()
	if ok {
		return time.Until(deadline), true
	}
	return 0, false
}
