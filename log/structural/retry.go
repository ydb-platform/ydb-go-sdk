package structural

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Retry returns trace.Retry with logging events from details
func Retry(l Logger, details trace.Details) (t trace.Retry) {
	if details&trace.RetryEvents == 0 {
		return
	}
	l = l.WithName(`retry`)
	t.OnRetry = func(
		info trace.RetryLoopStartInfo,
	) func(
		trace.RetryLoopIntermediateInfo,
	) func(
		trace.RetryLoopDoneInfo,
	) {
		id := info.ID
		idempotent := info.Idempotent
		l.Trace().
			String("id", id).
			Bool("idempotent", idempotent).
			Message("retry start")
		start := time.Now()
		return func(info trace.RetryLoopIntermediateInfo) func(trace.RetryLoopDoneInfo) {
			if info.Error == nil {
				l.Trace().
					String("id", id).
					Duration("latency", time.Since(start)).
					Message("retry attempt done")
			} else {
				var r Record
				if xerrors.IsYdb(info.Error) {
					r = l.Error()
				} else {
					r = l.Debug()
				}
				m := retry.Check(info.Error)
				r.
					String("id", id).
					Duration("latency", time.Since(start)).
					Error(info.Error).
					Bool("retryable", m.MustRetry(idempotent)).
					Int64("code", m.StatusCode()).
					Bool("deleteSession", m.MustDeleteSession()).
					String("version", meta.Version).
					Message("retry attempt failed")
			}
			return func(info trace.RetryLoopDoneInfo) {
				if info.Error == nil {
					l.Trace().
						String("id", id).
						Duration("latency", time.Since(start)).
						Int("attempts", info.Attempts).
						Message("retry done")
				} else {
					var r Record
					if xerrors.IsYdb(info.Error) {
						r = l.Error()
					} else {
						r = l.Debug()
					}
					m := retry.Check(info.Error)
					r.
						String("id", id).
						Duration("latency", time.Since(start)).
						Int("attempts", info.Attempts).
						Error(info.Error).
						Bool("retryable", m.MustRetry(idempotent)).
						Int64("code", m.StatusCode()).
						Bool("deleteSession", m.MustDeleteSession()).
						String("version", meta.Version).
						Message("retry failed")
				}
			}
		}
	}
	return t
}
