package structural

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Discovery makes trace.Discovery with logging events from details
func Discovery(l Logger, details trace.Details) (t trace.Discovery) {
	if details&trace.DiscoveryEvents == 0 {
		return
	}
	l = l.WithName(`discovery`)
	t.OnDiscover = func(info trace.DiscoveryDiscoverStartInfo) func(trace.DiscoveryDiscoverDoneInfo) {
		l.Info().
			String("address", info.Address).
			String("database", info.Database).
			Message("discover start")

		start := time.Now()
		return func(info trace.DiscoveryDiscoverDoneInfo) {
			if info.Error == nil {
				endpoints := make([]string, 0, len(info.Endpoints))
				for _, e := range info.Endpoints {
					endpoints = append(endpoints, e.String())
				}
				l.Debug().
					Duration("latency", time.Since(start)).
					Strings("endpoints", endpoints).
					Message("discover done")
			} else {
				l.Error().
					Duration("latency", time.Since(start)).
					Error(info.Error).
					String("version", meta.Version).
					Message("discover failed")
			}
		}
	}
	t.OnWhoAmI = func(info trace.DiscoveryWhoAmIStartInfo) func(doneInfo trace.DiscoveryWhoAmIDoneInfo) {
		l.Debug().Message(`whoAmI start`)
		start := time.Now()
		return func(info trace.DiscoveryWhoAmIDoneInfo) {
			if info.Error == nil {
				l.Debug().
					Duration("latency", time.Since(start)).
					String("user", info.User).
					Strings("groups", info.Groups).
					Message("whoAmI done")
			} else {
				l.Error().
					Duration("latency", time.Since(start)).
					Error(info.Error).
					String("version", meta.Version).
					Message("whoAmI failed")
			}
		}
	}
	return t
}
