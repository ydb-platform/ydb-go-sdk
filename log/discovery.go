package log

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Discovery(log Logger, details trace.Details) (t trace.Discovery) {
	if details&trace.DiscoveryEvents != 0 {
		log = log.WithName(`discovery`)
		t.OnDiscover = func(info trace.DiscoveryDiscoverStartInfo) func(trace.DiscoveryDiscoverDoneInfo) {
			log.Infof(`discover start {address:"%s",database:"%s"}`,
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
					log.Debugf(`discover done {latency:"%v",endpoints:%v}`,
						time.Since(start),
						endpoints,
					)
				} else {
					log.Errorf(`discover failed {latency:"%v",error:"%s"}`,
						time.Since(start),
						info.Error,
					)
				}
			}
		}
		t.OnWhoAmI = func(info trace.DiscoveryWhoAmIStartInfo) func(doneInfo trace.DiscoveryWhoAmIDoneInfo) {
			log.Debugf(`whoAmI start`)
			start := time.Now()
			return func(info trace.DiscoveryWhoAmIDoneInfo) {
				if info.Error == nil {
					log.Debugf(`whoAmI done {latency:"%v",user:%v,groups:%v}`,
						time.Since(start),
						info.User,
						info.Groups,
					)
				} else {
					log.Errorf(`whoAmI failed {latency:"%v",error:"%s"}`,
						time.Since(start),
						info.Error,
					)
				}
			}
		}
	}
	return t
}
