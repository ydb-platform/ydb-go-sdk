package spans

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/kv"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type (
	// KeyValue is key-value attribute for attaching into span
	KeyValue = kv.KeyValue

	// Span is an interface of spans in specific tracing system
	//
	// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
	Span interface {
		ID() (_ string, valid bool)
		TraceID() (_ string, valid bool)

		Link(link Span, attributes ...KeyValue)

		Log(msg string, attributes ...KeyValue)
		Warn(err error, attributes ...KeyValue)
		Error(err error, attributes ...KeyValue)

		End(attributes ...KeyValue)
	}

	// Adapter is interface of specific tracing system adapters
	//
	// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
	Adapter interface {
		trace.Detailer

		SpanFromContext(ctx context.Context) Span
		Start(ctx context.Context, operationName string, attributes ...KeyValue) (context.Context, Span)
	}
)
