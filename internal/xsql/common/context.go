package common

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stats"
)

type (
	ctxStatsModeKey struct{}

	StatsOption struct {
		Mode     options.StatsMode
		Callback func(stats.QueryStats)
	}
)

// WithStatsMode stores the requested stats mode and callback in the context.
func WithStatsMode(ctx context.Context, mode options.StatsMode, callback func(stats.QueryStats)) context.Context {
	return context.WithValue(ctx, ctxStatsModeKey{}, StatsOption{
		Mode:     mode,
		Callback: callback,
	})
}

// StatsModeFromContext extracts the stats mode and callback from the context.
// Returns (mode, callback, true) if set, or (0, nil, false) otherwise.
func StatsModeFromContext(ctx context.Context) (options.StatsMode, func(stats.QueryStats), bool) {
	v, ok := ctx.Value(ctxStatsModeKey{}).(StatsOption)
	if !ok {
		return 0, nil, false
	}

	return v.Mode, v.Callback, true
}
