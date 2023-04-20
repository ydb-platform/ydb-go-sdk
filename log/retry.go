package log

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Retry returns trace.Retry with logging events from details
func Retry(l Logger, d trace.Detailer) (t trace.Retry) {
	t.OnRetry = func(
		info trace.RetryLoopStartInfo,
	) func(
		trace.RetryLoopIntermediateInfo,
	) func(
		trace.RetryLoopDoneInfo,
	) {
		if d.Details()&trace.RetryEvents == 0 {
			return nil
		}
		ll := l.WithNames("retry")
		id := info.ID
		idempotent := info.Idempotent
		ll.Log(TRACE, "start",
			String("id", id),
			Bool("idempotent", idempotent),
		)
		start := time.Now()
		return func(info trace.RetryLoopIntermediateInfo) func(trace.RetryLoopDoneInfo) {
			if info.Error == nil {
				ll.Log(TRACE, "attempt done",
					String("id", id),
					latency(start),
				)
			} else {
				lvl := ERROR
				if !xerrors.IsYdb(info.Error) {
					lvl = DEBUG
				}
				m := retry.Check(info.Error)
				ll.Log(lvl, "attempt failed",
					Error(info.Error),
					String("id", id),
					latency(start),
					Bool("retryable", m.MustRetry(idempotent)),
					Int64("code", m.StatusCode()),
					Bool("deleteSession", m.MustDeleteSession()),
					version(),
				)
			}
			return func(info trace.RetryLoopDoneInfo) {
				if info.Error == nil {
					ll.Log(TRACE, "done",
						String("id", id),
						latency(start),
						Int("attempts", info.Attempts),
					)
				} else {
					lvl := ERROR
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					m := retry.Check(info.Error)
					ll.Log(lvl, "failed",
						Error(info.Error),
						String("id", id),
						latency(start),
						Int("attempts", info.Attempts),
						Bool("retryable", m.MustRetry(idempotent)),
						Int64("code", m.StatusCode()),
						Bool("deleteSession", m.MustDeleteSession()),
						version(),
					)
				}
			}
		}
	}
	return t
}
