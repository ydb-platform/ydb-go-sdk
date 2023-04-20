package log

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Scripting returns trace.Scripting with logging events from details
func Scripting(l Logger, d trace.Detailer, opts ...option) (t trace.Scripting) {
	options := parseOptions(opts...)
	t.OnExecute = func(info trace.ScriptingExecuteStartInfo) func(trace.ScriptingExecuteDoneInfo) {
		if d.Details()&trace.ScriptingEvents == 0 {
			return nil
		}
		ll := l.WithNames("scripting", "execute")
		ll.Log(DEBUG, "start")
		start := time.Now()
		return func(info trace.ScriptingExecuteDoneInfo) {
			if info.Error == nil {
				ll.Log(DEBUG, "done",
					latency(start),
					Int("resultSetCount", info.Result.ResultSetCount()),
					NamedError("resultSetError", info.Result.Err()),
				)
			} else {
				ll.Log(ERROR, "failed",
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
		ll := l.WithNames("scripting", "explain")
		ll.Log(TRACE, "start")
		start := time.Now()
		return func(info trace.ScriptingExplainDoneInfo) {
			if info.Error == nil {
				ll.Log(DEBUG, "done",
					latency(start),
					String("plan", info.Plan),
				)
			} else {
				ll.Log(ERROR, "failed",
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
		ll := l.WithNames("scripting", "stream", "execute")
		query := info.Query
		params := info.Parameters
		ll.Log(TRACE, "start",
			appendFieldByCondition(options.logQuery,
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
				ll.Log(TRACE, "intermediate")
			} else {
				ll.Log(WARN, "intermediate failed",
					Error(info.Error),
					version(),
				)
			}
			return func(info trace.ScriptingStreamExecuteDoneInfo) {
				if info.Error == nil {
					ll.Log(DEBUG, "done",
						latency(start),
						String("query", query),
						Stringer("params", params),
					)
				} else {
					ll.Log(ERROR, "failed",
						appendFieldByCondition(options.logQuery,
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
		ll := l.WithNames("scripting", "close")
		ll.Log(DEBUG, "start")
		start := time.Now()
		return func(info trace.ScriptingCloseDoneInfo) {
			if info.Error == nil {
				ll.Log(DEBUG, "done",
					latency(start),
				)
			} else {
				ll.Log(ERROR, "failed",
					Error(info.Error),
					latency(start),
					version(),
				)
			}
		}
	}
	return t
}
