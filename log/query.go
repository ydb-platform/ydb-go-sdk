package log

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Query makes trace.Query with logging events from details.
func Query(l Logger, d trace.Detailer, opts ...Option) (t trace.Query) {
	return internalQuery(wrapLogger(l, opts...), d)
}

func internalQuery(
	l *wrapper, //nolint:interfacer
	d trace.Detailer,
) (t trace.Query) {
	t.OnDo = func(
		info trace.QueryDoStartInfo,
	) func(
		info trace.QueryDoIntermediateInfo,
	) func(
		trace.QueryDoDoneInfo,
	) {
		if d.Details()&trace.QueryPoolEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "query", "do")
		l.Log(ctx, "start")
		start := time.Now()

		return func(info trace.QueryDoIntermediateInfo) func(trace.QueryDoDoneInfo) {
			if info.Error == nil {
				l.Log(ctx, "done",
					latencyField(start),
				)
			} else {
				lvl := WARN
				if !xerrors.IsYdb(info.Error) {
					lvl = DEBUG
				}
				l.Log(WithLevel(ctx, lvl), "failed",
					latencyField(start),
					Error(info.Error),
					versionField(),
				)
			}

			return func(info trace.QueryDoDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						latencyField(start),
						Int("attempts", info.Attempts),
					)
				} else {
					lvl := ERROR
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "done",
						latencyField(start),
						Error(info.Error),
						Int("attempts", info.Attempts),
						versionField(),
					)
				}
			}
		}
	}
	t.OnDoTx = func(
		info trace.QueryDoTxStartInfo,
	) func(
		info trace.QueryDoTxIntermediateInfo,
	) func(
		trace.QueryDoTxDoneInfo,
	) {
		if d.Details()&trace.TablePoolAPIEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "query", "do", "tx")
		l.Log(ctx, "start")
		start := time.Now()

		return func(info trace.QueryDoTxIntermediateInfo) func(trace.QueryDoTxDoneInfo) {
			if info.Error == nil {
				l.Log(ctx, "done",
					latencyField(start),
				)
			} else {
				lvl := WARN
				if !xerrors.IsYdb(info.Error) {
					lvl = DEBUG
				}
				l.Log(WithLevel(ctx, lvl), "done",
					latencyField(start),
					Error(info.Error),
					versionField(),
				)
			}

			return func(info trace.QueryDoTxDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						latencyField(start),
						Int("attempts", info.Attempts),
					)
				} else {
					lvl := ERROR
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "done",
						latencyField(start),
						Error(info.Error),
						Int("attempts", info.Attempts),
						versionField(),
					)
				}
			}
		}
	}

	return t
}
