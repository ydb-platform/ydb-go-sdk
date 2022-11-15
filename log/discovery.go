package log

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Discovery makes trace.Discovery with logging events from details
func Discovery(l Logger, details trace.Details) (t trace.Discovery) {
	if details&trace.DiscoveryEvents == 0 {
		return
	}
	l = l.WithName(`discovery`)
	t.OnDiscover = func(info trace.DiscoveryDiscoverStartInfo) func(trace.DiscoveryDiscoverDoneInfo) {
		l.Infof(`discover start {address:"%s",database:"%s"}`,
			info.Address,
			info.Database,
		)
		start := time.Now()
		return func(info trace.DiscoveryDiscoverDoneInfo) {
			if info.Error == nil {
				endpoints := make([]string, 0, len(info.Endpoints))
				for _, e := range info.Endpoints {
					endpoints = append(endpoints, e.String())
				}
				l.Debugf(`discover done {latency:"%v",endpoints:%v}`,
					time.Since(start),
					endpoints,
				)
			} else {
				l.Errorf(`discover failed {latency:"%v",error:"%s",version:"%s"}`,
					time.Since(start),
					info.Error,
					meta.Version,
				)
			}
		}
	}
	t.OnWhoAmI = func(info trace.DiscoveryWhoAmIStartInfo) func(doneInfo trace.DiscoveryWhoAmIDoneInfo) {
		l.Debugf(`whoAmI start`)
		start := time.Now()
		return func(info trace.DiscoveryWhoAmIDoneInfo) {
			if info.Error == nil {
				l.Debugf(`whoAmI done {latency:"%v",user:%v,groups:%v}`,
					time.Since(start),
					info.User,
					info.Groups,
				)
			} else {
				l.Errorf(`whoAmI failed {latency:"%v",error:"%s",version:"%s"}`,
					time.Since(start),
					info.Error,
					meta.Version,
				)
			}
		}
	}
	return t
}
