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
		if d.Details()&trace.QueryEvents == 0 {
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
					l.Log(WithLevel(ctx, lvl), "failed",
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
		if d.Details()&trace.QueryEvents == 0 {
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
					l.Log(WithLevel(ctx, lvl), "failed",
						latencyField(start),
						Error(info.Error),
						Int("attempts", info.Attempts),
						versionField(),
					)
				}
			}
		}
	}
	t.OnCreateSession = func(info trace.QueryCreateSessionStartInfo) func(info trace.QueryCreateSessionDoneInfo) {
		if d.Details()&trace.QuerySessionEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "query", "session", "create")
		l.Log(ctx, "start")
		start := time.Now()

		return func(info trace.QueryCreateSessionDoneInfo) {
			if info.Error == nil {
				l.Log(ctx, "done",
					latencyField(start),
					String("session_id", info.Session.ID()),
					String("session_status", info.Session.Status()),
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
		}
	}
	t.OnDeleteSession = func(info trace.QueryDeleteSessionStartInfo) func(info trace.QueryDeleteSessionDoneInfo) {
		if d.Details()&trace.QuerySessionEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "query", "session", "delete")
		l.Log(ctx, "start",
			String("session_id", info.Session.ID()),
			String("session_status", info.Session.Status()),
		)
		start := time.Now()

		return func(info trace.QueryDeleteSessionDoneInfo) {
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
		}
	}

	return t
}
