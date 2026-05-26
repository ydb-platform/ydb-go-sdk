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
