package xtest

import (
	"context"
	"runtime/pprof"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
)

func Context(tb testing.TB) context.Context {
	tb.Helper()
	ctx, cancel := xcontext.WithCancel(context.Background())
	ctx = pprof.WithLabels(ctx, pprof.Labels("test", tb.Name()))
	pprof.SetGoroutineLabels(ctx)

	tb.Cleanup(func() {
		pprof.SetGoroutineLabels(ctx)
		cancel()
	})
	return ctx
}

func ContextWithCommonTimeout(ctx context.Context, tb testing.TB) context.Context {
	tb.Helper()
	if ctx.Done() == nil {
		tb.Fatal("Use context with timeout only with context, cancelled on finish test, for example xtest.Context")
	}

	ctx, ctxCancel := xcontext.WithTimeout(ctx, commonWaitTimeout)
	_ = ctxCancel // suppress linters, it is ok for leak for small amount of time: it will cancel by parent context
	return ctx
}
