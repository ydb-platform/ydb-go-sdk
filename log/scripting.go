package log

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Scripting returns trace.Scripting with logging events from details
func Scripting(l Logger, d trace.Detailer, opts ...Option) (t trace.Scripting) {
	if ll, has := l.(*logger); has {
		return internalScripting(ll.with(opts...), d)
	}
	return internalScripting(New(append(opts, WithExternalLogger(l))...), d)
}

func internalScripting(l *logger, d trace.Detailer) (t trace.Scripting) {
	t.OnExecute = func(info trace.ScriptingExecuteStartInfo) func(trace.ScriptingExecuteDoneInfo) {
		if d.Details()&trace.ScriptingEvents == 0 {
			return nil
		}
		params := Params{
			Ctx:       *info.Context,
			Level:     TRACE,
			Namespace: []string{"scripting", "execute"},
		}
		l.Log(params.withLevel(DEBUG), "start")
		start := time.Now()
		return func(info trace.ScriptingExecuteDoneInfo) {
			if info.Error == nil {
				l.Log(params.withLevel(DEBUG), "done",
					latency(start),
					Int("resultSetCount", info.Result.ResultSetCount()),
					NamedError("resultSetError", info.Result.Err()),
				)
			} else {
				l.Log(params.withLevel(ERROR), "failed",
					Error(info.Error),
					latency(start),
					version(),
				)
			}
		}
	}
	t.OnExplain = func(info trace.ScriptingExplainStartInfo) func(trace.ScriptingExplainDoneInfo) {
		if d.Details()&trace.ScriptingEvents == 0 {
			return nil
		}
		params := Params{
			Ctx:       *info.Context,
			Level:     TRACE,
			Namespace: []string{"scripting", "explain"},
		}
		l.Log(params.withLevel(TRACE), "start")
		start := time.Now()
		return func(info trace.ScriptingExplainDoneInfo) {
			if info.Error == nil {
				l.Log(params.withLevel(DEBUG), "done",
					latency(start),
					String("plan", info.Plan),
				)
			} else {
				l.Log(params.withLevel(ERROR), "failed",
					Error(info.Error),
					latency(start),
					version(),
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
		params := Params{
			Ctx:       *info.Context,
			Level:     TRACE,
			Namespace: []string{"scripting", "stream", "execute"},
		}
		query := info.Query
		l.Log(params.withLevel(TRACE), "start",
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
				l.Log(params.withLevel(TRACE), "intermediate")
			} else {
				l.Log(params.withLevel(WARN), "intermediate failed",
					Error(info.Error),
					version(),
				)
			}
			return func(info trace.ScriptingStreamExecuteDoneInfo) {
				if info.Error == nil {
					l.Log(params.withLevel(DEBUG), "done",
						appendFieldByCondition(l.logQuery,
							String("query", query),
							latency(start),
						)...,
					)
				} else {
					l.Log(params.withLevel(ERROR), "failed",
						appendFieldByCondition(l.logQuery,
							String("query", query),
							Error(info.Error),
							latency(start),
							version(),
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
		params := Params{
			Ctx:       *info.Context,
			Level:     TRACE,
			Namespace: []string{"scripting", "close"},
		}
		l.Log(params.withLevel(DEBUG), "start")
		start := time.Now()
		return func(info trace.ScriptingCloseDoneInfo) {
			if info.Error == nil {
				l.Log(params.withLevel(DEBUG), "done",
					latency(start),
				)
			} else {
				l.Log(params.withLevel(ERROR), "failed",
					Error(info.Error),
					latency(start),
					version(),
				)
			}
		}
	}
	return t
}
