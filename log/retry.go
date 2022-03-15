package log

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Retry(l Logger, details trace.Details) (t trace.Retry) {
	// nolint:nestif
	if details&trace.RetryEvents != 0 {
		l = l.WithName(`retry`)
		t.OnRetry = func(
			info trace.RetryLoopStartInfo,
		) func(
			trace.RetryLoopIntermediateInfo,
		) func(
			trace.RetryLoopDoneInfo,
		) {
			id := info.ID
			l.Debugf(`retry start {id:"%s"}`, id)
			start := time.Now()
			return func(info trace.RetryLoopIntermediateInfo) func(trace.RetryLoopDoneInfo) {
				return func(info trace.RetryLoopDoneInfo) {
					if info.Error == nil {
						l.Debugf(`retry done {id:"%s",latency:"%v",attempts:%v}`,
							id,
							time.Since(start),
							info.Attempts,
						)
					} else {
						l.Errorf(`retry failed {id:"%s",latency:"%v",attempts:%v,error:"%s"}`,
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
