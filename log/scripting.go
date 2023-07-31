package log

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Scripting returns trace.Scripting with logging events from details
func Scripting(l Logger, d trace.Detailer, opts ...Option) (t trace.Scripting) {
	return internalScripting(wrapLogger(l, opts...), d)
}

func internalScripting(l *wrapper, d trace.Detailer) (t trace.Scripting) {
	t.OnExecute = func(info trace.ScriptingExecuteStartInfo) func(trace.ScriptingExecuteDoneInfo) {
		if d.Details()&trace.ScriptingEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "scripting", "execute")
		l.Log(ctx, "start")
		start := time.Now()
		return func(info trace.ScriptingExecuteDoneInfo) {
			if info.Error == nil {
				l.Log(ctx, "done",
					latencyField(start),
					Int("resultSetCount", info.Result.ResultSetCount()),
					NamedError("resultSetError", info.Result.Err()),
				)
			} else {
				l.Log(WithLevel(ctx, ERROR), "failed",
					Error(info.Error),
					latencyField(start),
					versionField(),
				)
			}
		}
	}
	t.OnExplain = func(info trace.ScriptingExplainStartInfo) func(trace.ScriptingExplainDoneInfo) {
		if d.Details()&trace.ScriptingEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "scripting", "explain")
		l.Log(ctx, "start")
		start := time.Now()
		return func(info trace.ScriptingExplainDoneInfo) {
			if info.Error == nil {
				l.Log(ctx, "done",
					latencyField(start),
					String("plan", info.Plan),
				)
			} else {
				l.Log(WithLevel(ctx, ERROR), "failed",
					Error(info.Error),
					latencyField(start),
					versionField(),
				)
			}
		}
	}
	t.OnStreamExecute = func(
		info trace.ScriptingStreamExecuteStartInfo,
	) func(
		trace.ScriptingStreamExecuteIntermediateInfo,
	) func(
		trace.ScriptingStreamExecuteDoneInfo,
	) {
		if d.Details()&trace.ScriptingEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "scripting", "stream", "execute")
		query := info.Query
		l.Log(ctx, "start",
			appendFieldByCondition(l.logQuery,
				String("query", query),
			)...,
		)
		start := time.Now()
		return func(
			info trace.ScriptingStreamExecuteIntermediateInfo,
		) func(
			trace.ScriptingStreamExecuteDoneInfo,
		) {
			if info.Error == nil {
				l.Log(ctx, "intermediate")
			} else {
				l.Log(WithLevel(ctx, WARN), "intermediate failed",
					Error(info.Error),
					versionField(),
				)
			}
			return func(info trace.ScriptingStreamExecuteDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						appendFieldByCondition(l.logQuery,
							String("query", query),
							latencyField(start),
						)...,
					)
				} else {
					l.Log(WithLevel(ctx, ERROR), "failed",
						appendFieldByCondition(l.logQuery,
							String("query", query),
							Error(info.Error),
							latencyField(start),
							versionField(),
						)...,
					)
				}
			}
		}
	}
	t.OnClose = func(info trace.ScriptingCloseStartInfo) func(trace.ScriptingCloseDoneInfo) {
		if d.Details()&trace.ScriptingEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "scripting", "close")
		l.Log(ctx, "start")
		start := time.Now()
		return func(info trace.ScriptingCloseDoneInfo) {
			if info.Error == nil {
				l.Log(ctx, "done",
					latencyField(start),
				)
			} else {
				l.Log(WithLevel(ctx, WARN), "failed",
					Error(info.Error),
					latencyField(start),
					versionField(),
				)
			}
		}
	}
	return t
}
