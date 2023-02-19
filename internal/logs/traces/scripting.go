package traces

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/logs"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Scripting returns trace.Scripting with logging events from details
func Scripting(l logs.Logger, details trace.Details, opts ...Option) (t trace.Scripting) {
	if details&trace.ScriptingEvents == 0 {
		return
	}
	options := ParseOptions(opts...)
	ll := newLogger(l, `scripting`)
	t.OnExecute = func(info trace.ScriptingExecuteStartInfo) func(trace.ScriptingExecuteDoneInfo) {
		ll.Debug(`execute start`)
		start := time.Now()
		return func(info trace.ScriptingExecuteDoneInfo) {
			if info.Error == nil {
				ll.Debug(`execute done`,
					latency(start),
					logs.Int("resultSetCount", info.Result.ResultSetCount()),
					logs.NamedError("resultSetError", info.Result.Err()),
				)
			} else {
				ll.Error(`execute failed`,
					logs.Error(info.Error),
					latency(start),
					version(),
				)
			}
		}
	}
	t.OnExplain = func(info trace.ScriptingExplainStartInfo) func(trace.ScriptingExplainDoneInfo) {
		ll.Debug(`explain start`)
		start := time.Now()
		return func(info trace.ScriptingExplainDoneInfo) {
			if info.Error == nil {
				ll.Debug(`explain done`,
					latency(start),
					logs.String("plan", info.Plan),
				)
			} else {
				ll.Error(`explain failed`,
					logs.Error(info.Error),
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
		query := info.Query
		params := info.Parameters
		if options.LogQuery {
			ll.Trace(`stream execute start`,
				logs.String("query", query),
				logs.Stringer("params", params),
			)
		} else {
			ll.Trace(`stream execute start`)
		}
		start := time.Now()
		return func(
			info trace.ScriptingStreamExecuteIntermediateInfo,
		) func(
			trace.ScriptingStreamExecuteDoneInfo,
		) {
			if info.Error == nil {
				ll.Trace(`stream execute intermediate`)
			} else {
				ll.Warn(`stream execute intermediate failed`,
					logs.Error(info.Error),
					version(),
				)
			}
			return func(info trace.ScriptingStreamExecuteDoneInfo) {
				if info.Error == nil {
					ll.Debug(`stream execute done`,
						latency(start),
						logs.String("query", query),
						logs.Stringer("params", params),
					)
				} else {
					if options.LogQuery {
						ll.Error(`stream execute failed`,
							logs.Error(info.Error),
							latency(start),
							logs.String("query", query),
							logs.Stringer("params", params),
							version(),
						)
					} else {
						ll.Error(`stream execute failed`,
							logs.Error(info.Error),
							latency(start),
							version(),
						)
					}
				}
			}
		}
	}
	t.OnClose = func(info trace.ScriptingCloseStartInfo) func(trace.ScriptingCloseDoneInfo) {
		ll.Debug(`close start`)
		start := time.Now()
		return func(info trace.ScriptingCloseDoneInfo) {
			if info.Error == nil {
				ll.Debug(`close done`,
					latency(start),
				)
			} else {
				ll.Error(`close failed {latency:"%v",error:"%s",version:"%s"}`,
					logs.Error(info.Error),
					latency(start),
					version(),
				)
			}
		}
	}
	return t
}
