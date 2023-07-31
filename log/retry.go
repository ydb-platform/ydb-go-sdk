package log

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Retry returns trace.Retry with logging events from details
func Retry(l Logger, d trace.Detailer, opts ...Option) (t trace.Retry) {
	return internalRetry(wrapLogger(l, opts...), d)
}

func internalRetry(l *wrapper, d trace.Detailer) (t trace.Retry) {
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
		ctx := with(*info.Context, TRACE, "ydb", "retry")
		id := info.ID
		idempotent := info.Idempotent
		l.Log(ctx, "start",
			String("id", id),
			Bool("idempotent", idempotent),
		)
		start := time.Now()
		return func(info trace.RetryLoopIntermediateInfo) func(trace.RetryLoopDoneInfo) {
			if info.Error == nil {
				l.Log(ctx, "attempt done",
					String("id", id),
					latencyField(start),
				)
			} else {
				lvl := ERROR
				if !xerrors.IsYdb(info.Error) {
					lvl = DEBUG
				}
				m := retry.Check(info.Error)
				l.Log(WithLevel(ctx, lvl), "attempt failed",
					Error(info.Error),
					String("id", id),
					latencyField(start),
					Bool("retryable", m.MustRetry(idempotent)),
					Int64("code", m.StatusCode()),
					Bool("deleteSession", m.MustDeleteSession()),
					versionField(),
				)
			}
			return func(info trace.RetryLoopDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						String("id", id),
						latencyField(start),
						Int("attempts", info.Attempts),
					)
				} else {
					lvl := ERROR
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					m := retry.Check(info.Error)
					l.Log(WithLevel(ctx, lvl), "failed",
						Error(info.Error),
						String("id", id),
						latencyField(start),
						Int("attempts", info.Attempts),
						Bool("retryable", m.MustRetry(idempotent)),
						Int64("code", m.StatusCode()),
						Bool("deleteSession", m.MustDeleteSession()),
						versionField(),
					)
				}
			}
		}
	}
	return t
}
