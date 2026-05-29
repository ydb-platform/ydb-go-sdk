package spans

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/kv"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

func childSpanWithReplaceCtx(
	cfg Adapter,
	ctx *context.Context,
	operationName string,
	fields ...KeyValue,
) (s Span) {
	*ctx, s = childSpan(cfg, *ctx, operationName, fields...)

	return s
}

func childSpan(
	cfg Adapter,
	ctx context.Context, //nolint:revive
	operationName string,
	fields ...KeyValue,
) (context.Context, Span) {
	return cfg.Start(ctx,
		operationName,
		fields...,
	)
}

func finish(
	s Span,
	err error,
	fields ...KeyValue,
) {
	if err != nil {
		setSpanError(s, err)
	}
	s.End(fields...)
}

// setSpanError marks the span as failed and attaches OTel-compliant error
// attributes.
//
//   - error.type
//   - db.response.status_code (only when the error carries a YDB status code)
//
// The `error.type` value is:
//   - "transport_error" for grpc transport errors,
//   - "ydb_error"       for any other ydb.Error,
//   - the Go dynamic type name (e.g. "*errors.errorString",
//     "context.deadlineExceededError") otherwise.
func setSpanError(s Span, err error) {
	s.Error(err, errorAttrs(err)...)
}

// errorAttrs returns OTel-compliant error attributes derived from err.
func errorAttrs(err error) []KeyValue {
	if err == nil {
		return nil
	}
	var (
		fields []KeyValue
		ydbErr ydb.Error
	)
	if xerrors.As(err, &ydbErr) {
		errorType := ErrorTypeYDB
		if xerrors.IsTransportError(err) {
			errorType = ErrorTypeTransport
		}
		fields = append(fields,
			kv.String(AttrErrorType, errorType),
			kv.Int(AttrDBResponseStatusCode, int(ydbErr.Code())),
		)

		return fields
	}
	fields = append(fields,
		kv.String(AttrErrorType, fmt.Sprintf("%T", err)),
	)

	return fields
}

func logError(s Span, err error, fields ...KeyValue) {
	fields = append(fields, errorAttrs(err)...)
	s.Error(err, fields...)
}

func logToParentSpan(
	cfg Adapter,
	ctx context.Context, //nolint:revive
	msg string,
	fields ...KeyValue,
) {
	parent := cfg.SpanFromContext(ctx)
	parent.Log(msg, fields...)
}

func logToParentSpanError(
	cfg Adapter,
	ctx context.Context, //nolint:revive
	err error,
	fields ...KeyValue,
) {
	parent := cfg.SpanFromContext(ctx)
	logError(parent, err, fields...)
}

type ctxClientSpanKey struct{}

// withClientSpan remembers s in ctx as the surrounding CLIENT-kind span so the
// driver layer can also attach network.peer.* / ydb.node.* attributes to it
// once the gRPC layer selects a concrete endpoint (see annotateNetworkPeer in
// driver.go).
//
// The outermost registration wins: nested calls do not overwrite an already
// registered span. This is needed because top-level Query / Exec / QueryRow /
// QueryResultSet handlers open a CLIENT span that is the parent of internal
// retry/try INTERNAL spans, and the actual RPC happens inside the inner
// INTERNAL spans — without this registration the outer client span would
// never receive ydb.node.id / ydb.node.dc.
func withClientSpan(ctx context.Context, s Span) context.Context {
	if _, has := ctx.Value(ctxClientSpanKey{}).(Span); has {
		return ctx
	}

	return context.WithValue(ctx, ctxClientSpanKey{}, s)
}

// clientSpanFromContext returns the outermost CLIENT-kind span registered in
// ctx via withClientSpan, or nil if no such span is registered.
func clientSpanFromContext(ctx context.Context) Span {
	s, _ := ctx.Value(ctxClientSpanKey{}).(Span)

	return s
}
