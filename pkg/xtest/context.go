package xtest

import (
	"context"
	"runtime/pprof"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
)

func Context(t testing.TB) context.Context {
	ctx, cancel := xcontext.WithCancel(context.Background())
	ctx = pprof.WithLabels(ctx, pprof.Labels("test", t.Name()))
	pprof.SetGoroutineLabels(ctx)

	t.Cleanup(func() {
		pprof.SetGoroutineLabels(ctx)
		cancel()
	})

	return ctx
}

func ContextWithCommonTimeout(ctx context.Context, t testing.TB) context.Context {
	if ctx.Done() == nil {
		t.Fatal("Use context with timeout only with context, cancelled on finish test, for example xtest.Context")
	}

	ctx, ctxCancel := xcontext.WithTimeout(ctx, commonWaitTimeout)
	_ = ctxCancel // suppress linters, it is ok for leak for small amount of time: it will cancel by parent context

	return ctx
}
