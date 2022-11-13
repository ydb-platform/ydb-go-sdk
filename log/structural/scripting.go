package structural

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Scripting returns trace.Scripting with logging events from details
func Scripting(l Logger, details trace.Details, opts ...Option) (t trace.Scripting) {
	if details&trace.ScriptingEvents == 0 {
		return
	}
	options := parseOptions(opts...)
	l = l.WithName(`scripting`)
	t.OnExecute = func(info trace.ScriptingExecuteStartInfo) func(trace.ScriptingExecuteDoneInfo) {
		l.Debug().Message("execute start")
		start := time.Now()
		return func(info trace.ScriptingExecuteDoneInfo) {
			if info.Error == nil {
				l.Debug().
					Duration("latency", time.Since(start)).
					Int("resultSetCount", info.Result.ResultSetCount()).
					NamedError("resultSetErr", info.Result.Err()).
					Message("execute done")
			} else {
				l.Error().
					Duration("latency", time.Since(start)).
					Error(info.Error).
					String("version", meta.Version).
					Message("execute failed")
			}
		}
	}
	t.OnExplain = func(info trace.ScriptingExplainStartInfo) func(trace.ScriptingExplainDoneInfo) {
		l.Debug().Message(`explain start`)
		start := time.Now()
		return func(info trace.ScriptingExplainDoneInfo) {
			if info.Error == nil {
				l.Debug().
					Duration("latency", time.Since(start)).
					String("plan", info.Plan).
					Message("explain done")
			} else {
				l.Error().
					Duration("latency", time.Since(start)).
					Error(info.Error).
					String("version", meta.Version).
					Message("explain failed")
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
		if options.logQuery {
			l.Trace().
				String("query", query).
				String("params", params.String()).
				Message("stream execute start")
		} else {
			l.Trace().Message(`stream execute start`)
		}
		start := time.Now()
		return func(
			info trace.ScriptingStreamExecuteIntermediateInfo,
		) func(
			trace.ScriptingStreamExecuteDoneInfo,
		) {
			if info.Error == nil {
				l.Trace().Message(`stream execute intermediate`)
			} else {
				l.Warn().
					Error(info.Error).
					String("version", meta.Version).
					Message("stream execute intermediate failed")
			}
			return func(info trace.ScriptingStreamExecuteDoneInfo) {
				//nolint:nestif
				if info.Error == nil {
					if options.logQuery {
						l.Debug().
							Duration("latency", time.Since(start)).
							String("query", query).
							String("params", params.String()).
							Message("stream execute done")
					} else {
						l.Debug().
							Duration("latency", time.Since(start)).
							Message("stream execute done")
					}
				} else {
					if options.logQuery {
						l.Error().
							String("query", query).
							String("params", params.String()).
							Error(info.Error).
							String("version", meta.Version).
							Message("stream execute failed")
					} else {
						l.Error().
							Error(info.Error).
							String("version", meta.Version).
							Message("stream execute failed")
					}
				}
			}
		}
	}
	t.OnClose = func(info trace.ScriptingCloseStartInfo) func(trace.ScriptingCloseDoneInfo) {
		l.Debug().Message("close start")
		start := time.Now()
		return func(info trace.ScriptingCloseDoneInfo) {
			if info.Error == nil {
				l.Debug().
					Duration("latency", time.Since(start)).
					Message("close done")
			} else {
				l.Error().
					Duration("latency", time.Since(start)).
					Error(info.Error).
					String("version", meta.Version).
					Message("close failed")
			}
		}
	}
	return t
}
