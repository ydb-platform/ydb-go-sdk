package traces

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/logs"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Discovery makes trace.Discovery with logging events from details
func Discovery(l logs.Logger, details trace.Details) (t trace.Discovery) {
	if details&trace.DiscoveryEvents == 0 {
		return
	}
	ll := newLogger(l, "discovery")
	t.OnDiscover = func(info trace.DiscoveryDiscoverStartInfo) func(trace.DiscoveryDiscoverDoneInfo) {
		ll.Info("discover start",
			logs.String("address", info.Address),
			logs.String("database", info.Database),
		)
		start := time.Now()
		return func(info trace.DiscoveryDiscoverDoneInfo) {
			if info.Error == nil {
				ll.Debug("discover done",
					latency(start),
					logs.Stringer("endpoints", endpoints(info.Endpoints)),
				)
			} else {
				ll.Error("discover failed",
					logs.Error(info.Error),
					latency(start),
					version(),
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
					latency(start),
					logs.String("user", info.User),
					logs.Strings("groups", info.Groups),
				)
			} else {
				ll.Error("whoAmI failed",
					logs.Error(info.Error),
					latency(start),
					version(),
				)
			}
		}
	}
	return t
}
