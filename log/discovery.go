package log

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Discovery makes trace.Discovery with logging events from details
func Discovery(l Logger, d trace.Detailer, opts ...Option) (t trace.Discovery) {
	return internalDiscovery(wrapLogger(l, opts...), d)
}

func internalDiscovery(l *wrapper, d trace.Detailer) (t trace.Discovery) {
	t.OnDiscover = func(info trace.DiscoveryDiscoverStartInfo) func(trace.DiscoveryDiscoverDoneInfo) {
		if d.Details()&trace.DiscoveryEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, DEBUG, "ydb", "discovery", "list", "endpoints")
		l.Log(ctx, "start",
			String("address", info.Address),
			String("database", info.Database),
		)
		start := time.Now()
		return func(info trace.DiscoveryDiscoverDoneInfo) {
			if info.Error == nil {
				l.Log(WithLevel(ctx, INFO), "done",
					latencyField(start),
					Stringer("endpoints", endpoints(info.Endpoints)),
				)
			} else {
				l.Log(WithLevel(ctx, ERROR), "failed",
					Error(info.Error),
					latencyField(start),
					versionField(),
				)
			}
		}
	}
	t.OnWhoAmI = func(info trace.DiscoveryWhoAmIStartInfo) func(doneInfo trace.DiscoveryWhoAmIDoneInfo) {
		if d.Details()&trace.DiscoveryEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "discovery", "whoAmI")
		l.Log(ctx, "start")
		start := time.Now()
		return func(info trace.DiscoveryWhoAmIDoneInfo) {
			if info.Error == nil {
				l.Log(ctx, "done",
					latencyField(start),
					String("user", info.User),
					Strings("groups", info.Groups),
				)
			} else {
				l.Log(WithLevel(ctx, WARN), "failed",
					Error(info.Error),
					latencyField(start),
					versionField(),
				)
			}
		}
	}
	return t
}
