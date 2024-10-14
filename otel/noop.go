package otel

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	_ Config = noopConfig{}
	_ Span   = noopSpan{}
)

type (
	noopConfig struct{}
	noopSpan   struct{}
)

func (noopSpan) TraceID() string {
	return ""
}

func (n noopSpan) Msg(string, ...KeyValue) {}

func (n noopSpan) End(...KeyValue) {}

func (noopConfig) Details() trace.Details {
	return 0
}

func (noopConfig) SpanFromContext(context.Context) Span {
	return noopSpan{}
}

func (noopConfig) Start(ctx context.Context, _ string, _ ...KeyValue) (context.Context, Span) {
	return ctx, noopSpan{}
}
