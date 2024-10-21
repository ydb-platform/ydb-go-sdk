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

		Link(link Span, attributes ...KeyValue)

		Log(msg string, attributes ...KeyValue)
		Warn(err error, attributes ...KeyValue)
		Error(err error, attributes ...KeyValue)

		End(attributes ...KeyValue)
	}
	Config interface {
		trace.Detailer

		SpanFromContext(ctx context.Context) Span
		Start(ctx context.Context, operationName string, attributes ...KeyValue) (context.Context, Span)
	}
)
