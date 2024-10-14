package otel

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/kv"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

//nolint:funlen
func scripting(cfg Config) (t trace.Scripting) {
	t.OnExecute = func(info trace.ScriptingExecuteStartInfo) func(trace.ScriptingExecuteDoneInfo) {
		if cfg.Details()&trace.ScriptingEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				info.Call.FunctionID(),
				kv.String("query", info.Query),
				kv.String("params", safeStringer(info.Parameters)),
			)

			return func(info trace.ScriptingExecuteDoneInfo) {
				if info.Error == nil {
					finish(
						start,
						safeErr(info.Result),
					)
				} else {
					finish(
						start,
						info.Error,
					)
				}
			}
		}

		return nil
	}
	t.OnStreamExecute = func(
		info trace.ScriptingStreamExecuteStartInfo,
	) func(
		trace.ScriptingStreamExecuteIntermediateInfo,
	) func(
		trace.ScriptingStreamExecuteDoneInfo,
	) {
		if cfg.Details()&trace.ScriptingEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				info.Call.FunctionID(),
				kv.String("query", info.Query),
				kv.String("params", safeStringer(info.Parameters)),
			)

			return func(
				info trace.ScriptingStreamExecuteIntermediateInfo,
			) func(
				trace.ScriptingStreamExecuteDoneInfo,
			) {
				start.Msg("", kv.Error(info.Error))

				return func(info trace.ScriptingStreamExecuteDoneInfo) {
					start.Msg("")
					if info.Error != nil {
						start.End(kv.Error(info.Error))
					} else {
						start.End()
					}
				}
			}
		}

		return nil
	}
	t.OnExplain = func(info trace.ScriptingExplainStartInfo) func(trace.ScriptingExplainDoneInfo) {
		if cfg.Details()&trace.ScriptingEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				info.Call.FunctionID(),
				kv.String("query", info.Query),
			)

			return func(info trace.ScriptingExplainDoneInfo) {
				finish(start, info.Error)
			}
		}

		return nil
	}
	t.OnClose = func(info trace.ScriptingCloseStartInfo) func(trace.ScriptingCloseDoneInfo) {
		if cfg.Details()&trace.ScriptingEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				info.Call.FunctionID(),
			)

			return func(info trace.ScriptingCloseDoneInfo) {
				finish(start, info.Error)
			}
		}

		return nil
	}

	return t
}
