package driver

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/cluster"
)

type ctxCallInfoKey struct{}

type callInfo func(cc cluster.Endpoint)

func WithCallInfo(ctx context.Context, info callInfo) context.Context {
	return context.WithValue(ctx, ctxCallInfoKey{}, info)
}

func ContextCallInfo(ctx context.Context) (info callInfo, ok bool) {
	if info, ok = ctx.Value(ctxCallInfoKey{}).(callInfo); ok {
		return info, true
	}
	return nil, false
}
