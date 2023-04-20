package log

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Discovery makes trace.Discovery with logging events from details
func Discovery(l Logger, d trace.Detailer) (t trace.Discovery) {
	t.OnDiscover = func(info trace.DiscoveryDiscoverStartInfo) func(trace.DiscoveryDiscoverDoneInfo) {
		if d.Details()&trace.DiscoveryEvents == 0 {
			return nil
		}
		ll := l.WithNames("discovery", "discover")
		ll.Log(INFO, "start",
			String("address", info.Address),
			String("database", info.Database),
		)
		start := time.Now()
		return func(info trace.DiscoveryDiscoverDoneInfo) {
			if info.Error == nil {
				ll.Log(DEBUG, "done",
					latency(start),
					Stringer("endpoints", endpoints(info.Endpoints)),
				)
			} else {
				ll.Log(ERROR, "failed",
					Error(info.Error),
					latency(start),
					version(),
				)
			}
		}
	}
	t.OnWhoAmI = func(info trace.DiscoveryWhoAmIStartInfo) func(doneInfo trace.DiscoveryWhoAmIDoneInfo) {
		if d.Details()&trace.DiscoveryEvents == 0 {
			return nil
		}
		ll := l.WithNames("discovery", "whoAmI")
		ll.Log(DEBUG, "start")
		start := time.Now()
		return func(info trace.DiscoveryWhoAmIDoneInfo) {
			if info.Error == nil {
				ll.Log(DEBUG, "done",
					latency(start),
					String("user", info.User),
					Strings("groups", info.Groups),
				)
			} else {
				ll.Log(ERROR, "failed",
					Error(info.Error),
					latency(start),
					version(),
				)
			}
		}
	}
	return t
}
