package log

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Scripting(l Logger, details trace.Details) (t trace.Scripting) {
	if details&trace.ScriptingEvents == 0 {
		return
	}
	l = l.WithName(`scripting`)
	t.OnExecute = func(info trace.ScriptingExecuteStartInfo) func(trace.ScriptingExecuteDoneInfo) {
		l.Debugf(`execute start`)
		start := time.Now()
		return func(info trace.ScriptingExecuteDoneInfo) {
			if info.Error == nil {
				l.Debugf(`execute done {latency:"%v",resultSetCount:%v,resultSetErr:"%v""}`,
					time.Since(start),
					info.Result.ResultSetCount(),
					info.Result.Err(),
				)
			} else {
				l.Errorf(`execute failed {latency:"%v",error:"%s"}`,
					time.Since(start),
					info.Error,
				)
			}
		}
	}
	t.OnExplain = func(info trace.ScriptingExplainStartInfo) func(trace.ScriptingExplainDoneInfo) {
		l.Debugf(`explain start`)
		start := time.Now()
		return func(info trace.ScriptingExplainDoneInfo) {
			if info.Error == nil {
				l.Debugf(`explain done {latency:"%v",plan:%v"}`,
					time.Since(start),
					info.Plan,
				)
			} else {
				l.Errorf(`explain failed {latency:"%v",error:"%s"}`,
					time.Since(start),
					info.Error,
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
		l.Tracef(`stream execute start {query:"%s",params:"%s"}`,
			query,
			params,
		)
		start := time.Now()
		return func(
			info trace.ScriptingStreamExecuteIntermediateInfo,
		) func(
			trace.ScriptingStreamExecuteDoneInfo,
		) {
			if info.Error == nil {
				l.Tracef(`stream execute intermediate`)
			} else {
				l.Warnf(`stream execute intermediate failed {error:"%v",version:"%s"}`,
					info.Error,
					meta.Version,
				)
			}
			return func(info trace.ScriptingStreamExecuteDoneInfo) {
				if info.Error == nil {
					l.Debugf(`stream execute done {latency:"%v",query:"%s",params:"%s"}`,
						time.Since(start),
						query,
						params,
					)
				} else {
					l.Errorf(`stream execute failed {latency:"%v",query:"%s",params:"%s",error:"%v"}`,
						time.Since(start),
						query,
						params,
						info.Error,
					)
				}
			}
		}
	}
	t.OnClose = func(info trace.ScriptingCloseStartInfo) func(trace.ScriptingCloseDoneInfo) {
		l.Debugf(`close start`)
		start := time.Now()
		return func(info trace.ScriptingCloseDoneInfo) {
			if info.Error == nil {
				l.Debugf(`close done {latency:"%v"}`,
					time.Since(start),
				)
			} else {
				l.Errorf(`close failed {latency:"%v",error:"%s"}`,
					time.Since(start),
					info.Error,
				)
			}
		}
	}
	return t
}
