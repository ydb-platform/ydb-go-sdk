package xtest

import (
	"context"
	"fmt"
	"runtime/pprof"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
)

func Context(t testing.TB) context.Context {
	ctx, cancel := xcontext.WithErrCancel(context.Background())
	ctx = pprof.WithLabels(ctx, pprof.Labels("test", t.Name()))
	pprof.SetGoroutineLabels(ctx)

	t.Cleanup(func() {
		pprof.SetGoroutineLabels(ctx)
		cancel(fmt.Errorf("test context finished: %v", t.Name()))
	})
	return ctx
}
