package bench_test

import (
	"context"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func BenchmarkTraceQuery(b *testing.B) {
	t := &trace.Query{} // empty trace
	ctx := context.Background()
	b.ReportAllocs()
	for range b.N {
		onDone := trace.QueryOnSessionQuery(t, &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Session).Query"),
			nil, "SELECT 42", "",
		)
		onDone(nil)
	}
}
