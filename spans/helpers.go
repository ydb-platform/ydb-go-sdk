package spans

import (
	"context"

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
		s.Error(err)
	}
	s.End(fields...)
}

func logError(s Span, err error, fields ...KeyValue) {
	var ydbErr ydb.Error
	if xerrors.As(err, &ydbErr) {
		fields = append(fields,
			kv.Int("error.ydb.code", int(ydbErr.Code())),
			kv.String("error.ydb.name", ydbErr.Name()),
		)
	}
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
