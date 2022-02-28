package log

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Retry(log Logger, details trace.Details) (t trace.Retry) {
	if details&trace.RetryEvents != 0 {
		log = log.WithName(`retry`)
		t.OnRetry = func(
			info trace.RetryLoopStartInfo,
		) func(
			trace.RetryLoopIntermediateInfo,
		) func(
			trace.RetryLoopDoneInfo,
		) {
			id := info.ID
			log.Debugf(`retry start {id:"%s"}`, id)
			start := time.Now()
			return func(info trace.RetryLoopIntermediateInfo) func(trace.RetryLoopDoneInfo) {
				return func(info trace.RetryLoopDoneInfo) {
					if info.Error == nil {
						log.Debugf(`retry done {id:"%s",latency:"%v",attempts:%v}`,
							id,
							time.Since(start),
							info.Attempts,
						)
					} else {
						log.Errorf(`retry failed {id:"%s",latency:"%v",attempts:%v,error:"%s"}`,
							id,
							time.Since(start),
							info.Attempts,
							info.Error,
						)
					}
				}
			}
		}
	}
	return t
}
