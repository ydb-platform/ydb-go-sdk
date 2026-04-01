package common

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stats"
)

type StatsMode uint8

const (
	StatsModeBasic   StatsMode = iota + 1
	StatsModeFull
	StatsModeProfile
)

type ctxStatsModeKey struct{}

type StatsOptions struct {
	Mode     StatsMode
	Callback func(stats.QueryStats)
}

func WithStatsMode(ctx context.Context, mode StatsMode, callback func(stats.QueryStats)) context.Context {
	return context.WithValue(ctx, ctxStatsModeKey{}, &StatsOptions{
		Mode:     mode,
		Callback: callback,
	})
}

func StatsModeFromContext(ctx context.Context) *StatsOptions {
	if v, ok := ctx.Value(ctxStatsModeKey{}).(*StatsOptions); ok {
		return v
	}

	return nil
}
