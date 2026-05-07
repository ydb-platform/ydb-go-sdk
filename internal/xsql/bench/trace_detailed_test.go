package bench_test

import (
	"context"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func BenchmarkTraceQueryDetailed(b *testing.B) {
	t := &trace.Query{} // empty trace
	ctx := context.Background()
	callID := stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Session).Query")
	b.ReportAllocs()
	for range b.N {
		onDone := trace.QueryOnSessionQuery(t, &ctx, callID, nil, "SELECT 42", "")
		onDone(nil)
	}
}

func BenchmarkFunctionIDAlloc(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		f := stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Session).Query")
		_ = f
	}
}

func BenchmarkFunctionIDWithPackage(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		f := stack.FunctionID("database/sql.(*Conn).QueryContext", stack.Package("database/sql"))
		_ = f
	}
}

var _cachedCallID = stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Session).Query")

func BenchmarkTraceQueryCachedCall(b *testing.B) {
	t := &trace.Query{}
	ctx := context.Background()
	b.ReportAllocs()
	for range b.N {
		onDone := trace.QueryOnSessionQuery(t, &ctx, _cachedCallID, nil, "SELECT 42", "")
		onDone(nil)
	}
}
