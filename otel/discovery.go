package otel

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/kv"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func discovery(cfg Config) (t trace.Discovery) {
	t.OnDiscover = func(info trace.DiscoveryDiscoverStartInfo) func(discovery trace.DiscoveryDiscoverDoneInfo) {
		if cfg.Details()&trace.DiscoveryEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				info.Call.FunctionID(),
				kv.String("address", info.Address),
				kv.String("database", info.Database),
			)

			return func(info trace.DiscoveryDiscoverDoneInfo) {
				endpoints := make([]string, len(info.Endpoints))
				for i, e := range info.Endpoints {
					endpoints[i] = e.String()
				}
				finish(
					start,
					info.Error,
					kv.Strings("endpoints", endpoints),
				)
			}
		}

		return nil
	}

	return t
}
