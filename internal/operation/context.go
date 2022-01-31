package operation

import (
	"context"
	"fmt"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/testutil/timeutil"
)

func ContextParams(ctx context.Context) (p Params) {
	var hasOpTimeout bool

	p.Mode, _ = ContextMode(ctx)
	p.Timeout, hasOpTimeout = ContextTimeout(ctx)
	p.CancelAfter, _ = ContextCancelAfter(ctx)

	if p.Mode != ModeSync {
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
		return ydb_testutil_timeutil.Until(deadline), true
	}
	return 0, false
}

type ctxOpTimeoutKey struct{}

type ctxOpCancelAfterKey struct{}

type ctxOpModeKey struct{}

// WithTimeout returns a copy of parent deadline in which YDB operation timeout
// parameter is set to d. If parent deadline timeout is smaller than d, parent deadline
// is returned.
func WithTimeout(ctx context.Context, d time.Duration) context.Context {
	if cur, ok := ContextTimeout(ctx); ok && d >= cur {
		// The current timeout is already smaller than the new one.
		return ctx
	}
	return context.WithValue(ctx, ctxOpTimeoutKey{}, d)
}

// ContextTimeout returns the timeout within given deadline after which
// YDB should try to cancel operation and return result regardless of the
// cancelation.
func ContextTimeout(ctx context.Context) (d time.Duration, ok bool) {
	d, ok = ctx.Value(ctxOpTimeoutKey{}).(time.Duration)
	return
}

// WithCancelAfter returns a copy of parent deadline in which YDB operation
// cancel after parameter is set to d. If parent deadline cancelation timeout is smaller
// than d, parent deadline deadline is returned.
func WithCancelAfter(ctx context.Context, d time.Duration) context.Context {
	if cur, ok := ContextCancelAfter(ctx); ok && d >= cur {
		// The current cancelation timeout is already smaller than the new one.
		return ctx
	}
	return context.WithValue(ctx, ctxOpCancelAfterKey{}, d)
}

// ContextCancelAfter returns the timeout within given deadline after which
// YDB should try to cancel operation and return result regardless of the
// cancelation.
func ContextCancelAfter(ctx context.Context) (d time.Duration, ok bool) {
	d, ok = ctx.Value(ctxOpCancelAfterKey{}).(time.Duration)
	return
}

// WithMode returns a copy of parent deadline in which YDB operation mode
// parameter is set to m. If parent deadline mode is set and is not equal to m,
// WithMode will panic.
func WithMode(ctx context.Context, m Mode) context.Context {
	if cur, ok := ContextMode(ctx); ok {
		if cur != m {
			panic(fmt.Sprintf(
				"ydb: deadline already has different operation mode: %v; %v given",
				cur, m,
			))
		}
		return ctx
	}
	return context.WithValue(ctx, ctxOpModeKey{}, m)
}

// ContextMode returns the mode of YDB operation within given deadline.
func ContextMode(ctx context.Context) (m Mode, ok bool) {
	m, ok = ctx.Value(ctxOpModeKey{}).(Mode)
	return
}
