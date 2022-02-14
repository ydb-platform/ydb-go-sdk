package log

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Discovery(log Logger, details trace.Details) (t trace.Discovery) {
	if details&trace.DiscoveryEvents != 0 {
		log = log.WithName(`discovery`)
		t.OnDiscover = func(info trace.DiscoverStartInfo) func(trace.DiscoverDoneInfo) {
			log.Infof(`discover start {address:"%s",database:"%s"}`,
				info.Address,
				info.Database,
			)
			start := time.Now()
			return func(info trace.DiscoverDoneInfo) {
				if info.Error == nil {
					log.Debugf(`discover done {latency:"%s",endpoints:%v}`,
						time.Since(start),
						info.Endpoints,
					)
				} else {
					log.Errorf(`discover failed {latency:"%s",error:"%s"}`,
						time.Since(start),
						info.Error,
					)
				}
			}
		}
		t.OnWhoAmI = func(info trace.WhoAmIStartInfo) func(doneInfo trace.WhoAmIDoneInfo) {
			log.Debugf(`whoAmI start`)
			start := time.Now()
			return func(info trace.WhoAmIDoneInfo) {
				if info.Error == nil {
					log.Debugf(`whoAmI done {latency:"%s",user:%v,groups:%v}`,
						time.Since(start),
						info.User,
						info.Groups,
					)
				} else {
					log.Errorf(`whoAmI failed {latency:"%s",error:"%s"}`,
						time.Since(start),
						info.Error,
					)
				}
			}
		}
	}
	return t
}
