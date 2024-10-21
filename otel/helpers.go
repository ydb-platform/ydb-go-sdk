package otel

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/kv"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

func childSpanWithReplaceCtx(
	cfg Config,
	ctx *context.Context,
	operationName string,
	fields ...KeyValue,
) (s Span) {
	*ctx, s = childSpan(cfg, *ctx, operationName, fields...)

	return s
}

func childSpan(
	cfg Config,
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
		s.Msg(err.Error(), kv.Error(err))
	}
	s.End(fields...)
}

func logError(
	s Span,
	err error,
	fields ...KeyValue,
) {
	var ydbErr ydb.Error
	if xerrors.As(err, &ydbErr) {
		fields = append(fields,
			kv.Error(err),
			kv.Int("error.ydb.code", int(ydbErr.Code())),
			kv.String("error.ydb.name", ydbErr.Name()),
		)
	}
	s.Msg(err.Error(), fields...)
}

func logToParentSpan(
	cfg Config,
	ctx context.Context, //nolint:revive
	msg string,
	fields ...KeyValue, //nolint:unparam
) {
	parent := cfg.SpanFromContext(ctx)
	parent.Msg(msg, fields...)
}

func logToParentSpanError(
	cfg Config,
	ctx context.Context, //nolint:revive
	err error,
	fields ...KeyValue, //nolint:unparam
) {
	parent := cfg.SpanFromContext(ctx)
	logError(parent, err, fields...)
}
