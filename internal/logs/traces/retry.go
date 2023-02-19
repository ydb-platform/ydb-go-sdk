package traces

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/logs"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Retry returns trace.Retry with logging events from details
func Retry(l logs.Logger, details trace.Details) (t trace.Retry) {
	if details&trace.RetryEvents == 0 {
		return
	}
	ll := newLogger(l, "retry")
	t.OnRetry = func(
		info trace.RetryLoopStartInfo,
	) func(
		trace.RetryLoopIntermediateInfo,
	) func(
		trace.RetryLoopDoneInfo,
	) {
		id := info.ID
		idempotent := info.Idempotent
		ll.Trace(`retry start`,
			logs.String("id", id),
			logs.Bool("idempotent", idempotent),
		)
		start := time.Now()
		return func(info trace.RetryLoopIntermediateInfo) func(trace.RetryLoopDoneInfo) {
			if info.Error == nil {
				ll.Trace(`retry attempt done`,
					logs.String("id", id),
					latency(start),
				)
			} else {
				f := ll.Error
				if !xerrors.IsYdb(info.Error) {
					f = ll.Debug
				}
				m := retry.Check(info.Error)
				f("retry attempt failed",
					logs.Error(info.Error),
					logs.String("id", id),
					latency(start),
					logs.Bool("retryable", m.MustRetry(idempotent)),
					logs.Int64("code", m.StatusCode()),
					logs.Bool("deleteSession", m.MustDeleteSession()),
					version(),
				)
			}
			return func(info trace.RetryLoopDoneInfo) {
				if info.Error == nil {
					ll.Trace(`retry done`,
						logs.String("id", id),
						latency(start),
						logs.Int("attempts", info.Attempts),
					)
				} else {
					f := ll.Error
					if !xerrors.IsYdb(info.Error) {
						f = ll.Debug
					}
					m := retry.Check(info.Error)
					f("retry failed",
						logs.Error(info.Error),
						logs.String("id", id),
						latency(start),
						logs.Int("attempts", info.Attempts),
						logs.Bool("retryable", m.MustRetry(idempotent)),
						logs.Int64("code", m.StatusCode()),
						logs.Bool("deleteSession", m.MustDeleteSession()),
						version(),
					)
				}
			}
		}
	}
	return t
}
