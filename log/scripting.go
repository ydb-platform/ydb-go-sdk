package log

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Scripting(log Logger, details trace.Details) (t trace.Scripting) {
	// nolint:nestif
	if details&trace.ScriptingEvents != 0 {
		log = log.WithName(`scripting`)
		t.OnExecute = func(info trace.ScriptingExecuteStartInfo) func(trace.ScriptingExecuteDoneInfo) {
			log.Debugf(`execute start`)
			start := time.Now()
			return func(info trace.ScriptingExecuteDoneInfo) {
				if info.Error == nil {
					log.Debugf(`execute done {latency:"%v",resultSetCount:%v,resultSetErr:"%v""}`,
						time.Since(start),
						info.Result.ResultSetCount(),
						info.Result.Err(),
					)
				} else {
					log.Errorf(`execute failed {latency:"%v",error:"%s"}`,
						time.Since(start),
						info.Error,
					)
				}
			}
		}
		t.OnExplain = func(info trace.ScriptingExplainStartInfo) func(trace.ScriptingExplainDoneInfo) {
			log.Debugf(`explain start`)
			start := time.Now()
			return func(info trace.ScriptingExplainDoneInfo) {
				if info.Error == nil {
					log.Debugf(`explain done {latency:"%v",plan:%v"}`,
						time.Since(start),
						info.Plan,
					)
				} else {
					log.Errorf(`explain failed {latency:"%v",error:"%s"}`,
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
			log.Tracef(`stream execute start {query:"%s",params:"%s"}`,
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
					log.Tracef(`stream execute intermediate`)
				} else {
					log.Warnf(`stream execute intermediate failed {error:"%v"}`,
						info.Error,
					)
				}
				return func(info trace.ScriptingStreamExecuteDoneInfo) {
					if info.Error == nil {
						log.Debugf(`stream execute done {latency:"%v",query:"%s",params:"%s"}`,
							time.Since(start),
							query,
							params,
						)
					} else {
						log.Errorf(`stream execute failed {latency:"%v",query:"%s",params:"%s",error:"%v"}`,
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
			log.Debugf(`close start`)
			start := time.Now()
			return func(info trace.ScriptingCloseDoneInfo) {
				if info.Error == nil {
					log.Debugf(`close done {latency:"%v"}`,
						time.Since(start),
					)
				} else {
					log.Errorf(`close failed {latency:"%v",error:"%s"}`,
						time.Since(start),
						info.Error,
					)
				}
			}
		}
		t.OnInit = func(info trace.ScriptingInitStartInfo) func(trace.ScriptingInitDoneInfo) {
			log.Debugf(`init start`)
			start := time.Now()
			return func(info trace.ScriptingInitDoneInfo) {
				if info.Error == nil {
					log.Debugf(`init done {latency:"%v"}`,
						time.Since(start),
					)
				} else {
					log.Errorf(`init failed {latency:"%v",error:"%s"}`,
						time.Since(start),
						info.Error,
					)
				}
			}
		}
	}
	return t
}
