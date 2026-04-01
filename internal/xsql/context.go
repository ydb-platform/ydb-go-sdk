package xsql

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stats"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/common"
)

type ctxExplainQueryModeKey struct{}

func WithExplain(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxExplainQueryModeKey{}, true)
}

func isExplain(ctx context.Context) bool {
	v, has := ctx.Value(ctxExplainQueryModeKey{}).(bool)

	return has && v
}

// WithStatsMode stores the requested stats mode and callback in the context.
func WithStatsMode(ctx context.Context, mode options.StatsMode, callback func(stats.QueryStats)) context.Context {
	return common.WithStatsMode(ctx, mode, callback)
}
