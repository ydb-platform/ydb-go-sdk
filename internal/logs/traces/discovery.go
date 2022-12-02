package traces

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/logs"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Discovery makes trace.Discovery with logging events from details
func Discovery(l logs.Logger, details trace.Details) (t trace.Discovery) {
	if details&trace.DiscoveryEvents == 0 {
		return
	}
	scope := []string{"discovery"}
	ll := logger{
		l:     l,
		scope: scope,
	}
	t.OnDiscover = func(info trace.DiscoveryDiscoverStartInfo) func(trace.DiscoveryDiscoverDoneInfo) {
		ll.Info("discover start",
			logs.String("address", info.Address),
			logs.String("database", info.Database),
		)
		start := time.Now()
		return func(info trace.DiscoveryDiscoverDoneInfo) {
			if info.Error == nil {
				ll.Debug("discover done",
					logs.Duration("latency", time.Since(start)),
					logs.Endpoints("endpoints", info.Endpoints),
				)
			} else {
				ll.Error("discover failed",
					logs.Duration("latency", time.Since(start)),
					logs.Error(info.Error),
					logs.String("version", meta.Version),
				)
			}
		}
	}
	t.OnWhoAmI = func(info trace.DiscoveryWhoAmIStartInfo) func(doneInfo trace.DiscoveryWhoAmIDoneInfo) {
		ll.Debug("whoAmI start")
		start := time.Now()
		return func(info trace.DiscoveryWhoAmIDoneInfo) {
			if info.Error == nil {
				ll.Debug("whoAmI done",
					logs.Duration("latency", time.Since(start)),
					logs.String("user", info.User),
					logs.Strings("groups", info.Groups),
				)
			} else {
				ll.Error("whoAmI failed",
					logs.Duration("latency", time.Since(start)),
					logs.Error(info.Error),
					logs.String("version", meta.Version),
				)
			}
		}
	}
	return t
}
