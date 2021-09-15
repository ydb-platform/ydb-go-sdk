package operation

import (
	"context"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/timeutil"
	"time"
)

func ContextParams(ctx context.Context) (p Params) {
	var hasOpTimeout bool

	p.Mode, _ = ContextOperationMode(ctx)
	p.Timeout, hasOpTimeout = ContextOperationTimeout(ctx)
	p.CancelAfter, _ = ContextOperationCancelAfter(ctx)

	if p.Mode != OperationModeSync {
		return
	}

	deadline, hasDeadline := contextUntilDeadline(ctx)
	if !hasDeadline {
		return
	}

	if hasOpTimeout && p.Timeout <= deadline {
		return
	}

	p.Timeout = deadline

	return
}

func contextUntilDeadline(ctx context.Context) (time.Duration, bool) {
	deadline, ok := ctx.Deadline()
	if ok {
		return timeutil.Until(deadline), true
	}
	return 0, false
}

type ctxOpTimeoutKey struct{}

type ctxOpCancelAfterKey struct{}

type ctxOpModeKey struct{}

// WithOperationTimeout returns a copy of parent context in which YDB operation timeout
// parameter is set to d. If parent context timeout is smaller than d, parent context
// is returned.
func WithOperationTimeout(ctx context.Context, d time.Duration) context.Context {
	if cur, ok := ContextOperationTimeout(ctx); ok && d >= cur {
		// The current timeout is already smaller than the new one.
		return ctx
	}
	return context.WithValue(ctx, ctxOpTimeoutKey{}, d)
}

// ContextOperationTimeout returns the timeout within given context after which
// YDB should try to cancel operation and return result regardless of the
// cancelation.
func ContextOperationTimeout(ctx context.Context) (d time.Duration, ok bool) {
	d, ok = ctx.Value(ctxOpTimeoutKey{}).(time.Duration)
	return
}

// WithOperationCancelAfter returns a copy of parent context in which YDB operation
// cancel after parameter is set to d. If parent context cancelation timeout is smaller
// than d, parent context context is returned.
func WithOperationCancelAfter(ctx context.Context, d time.Duration) context.Context {
	if cur, ok := ContextOperationCancelAfter(ctx); ok && d >= cur {
		// The current cancelation timeout is already smaller than the new one.
		return ctx
	}
	return context.WithValue(ctx, ctxOpCancelAfterKey{}, d)
}

// ContextOperationCancelAfter returns the timeout within given context after which
// YDB should try to cancel operation and return result regardless of the
// cancelation.
func ContextOperationCancelAfter(ctx context.Context) (d time.Duration, ok bool) {
	d, ok = ctx.Value(ctxOpCancelAfterKey{}).(time.Duration)
	return
}

// WithOperationMode returns a copy of parent context in which YDB operation mode
// parameter is set to m. If parent context mode is set and is not equal to m,
// WithOperationMode will panic.
func WithOperationMode(ctx context.Context, m OperationMode) context.Context {
	if cur, ok := ContextOperationMode(ctx); ok {
		if cur != m {
			panic(fmt.Sprintf(
				"ydb: context already has different operation mode: %v; %v given",
				cur, m,
			))
		}
		return ctx
	}
	return context.WithValue(ctx, ctxOpModeKey{}, m)
}

// ContextOperationMode returns the mode of YDB operation within given context.
func ContextOperationMode(ctx context.Context) (m OperationMode, ok bool) {
	m, ok = ctx.Value(ctxOpModeKey{}).(OperationMode)
	return
}
