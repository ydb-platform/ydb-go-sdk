package log

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Discovery makes trace.Discovery with logging events from details
func Discovery(l Logger, d trace.Detailer, opts ...Option) (t trace.Discovery) {
	if ll, has := l.(*logger); has {
		return internalDiscovery(ll.with(opts...), d)
	}
	return internalDiscovery(New(append(opts, WithExternalLogger(l))...), d)
}

func internalDiscovery(l *logger, d trace.Detailer) (t trace.Discovery) {
	t.OnDiscover = func(info trace.DiscoveryDiscoverStartInfo) func(trace.DiscoveryDiscoverDoneInfo) {
		if d.Details()&trace.DiscoveryEvents == 0 {
			return nil
		}
		params := Params{
			Ctx:       *info.Context,
			Level:     DEBUG,
			Namespace: []string{"discovery", "list", "endpoints"},
		}
		l.Log(params, "start",
			String("address", info.Address),
			String("database", info.Database),
		)
		start := time.Now()
		return func(info trace.DiscoveryDiscoverDoneInfo) {
			if info.Error == nil {
				l.Log(params.withLevel(INFO), "done",
					latency(start),
					Stringer("endpoints", endpoints(info.Endpoints)),
				)
			} else {
				l.Log(params.withLevel(ERROR), "failed",
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
		params := Params{
			Ctx:       *info.Context,
			Level:     TRACE,
			Namespace: []string{"discovery", "whoAmI"},
		}
		l.Log(params, "start")
		start := time.Now()
		return func(info trace.DiscoveryWhoAmIDoneInfo) {
			if info.Error == nil {
				l.Log(params.withLevel(TRACE), "done",
					latency(start),
					String("user", info.User),
					Strings("groups", info.Groups),
				)
			} else {
				l.Log(params.withLevel(WARN), "failed",
					Error(info.Error),
					latency(start),
					version(),
				)
			}
		}
	}
	return t
}
