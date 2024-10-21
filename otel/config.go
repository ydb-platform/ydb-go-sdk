package otel

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/kv"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type (
	KeyValue = kv.KeyValue
	Span     interface {
		TraceID() string

		Relation(span Span)

		Msg(msg string, attributes ...KeyValue)

		End(attributes ...KeyValue)
	}
	Config interface {
		trace.Detailer

		SpanFromContext(ctx context.Context) Span
		Start(ctx context.Context, operationName string, attributes ...KeyValue) (context.Context, Span)
	}
)
