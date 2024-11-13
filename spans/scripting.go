package spans

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/kv"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

//nolint:funlen
func scripting(adapter Adapter) (t trace.Scripting) {
	t.OnExecute = func(info trace.ScriptingExecuteStartInfo) func(trace.ScriptingExecuteDoneInfo) {
		if adapter.Details()&trace.ScriptingEvents != 0 {
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
				kv.String("query", info.Query),
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
		if adapter.Details()&trace.ScriptingEvents != 0 {
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
				kv.String("query", info.Query),
			)

			return func(
				info trace.ScriptingStreamExecuteIntermediateInfo,
			) func(
				trace.ScriptingStreamExecuteDoneInfo,
			) {
				if info.Error != nil {
					start.Warn(info.Error)
				}

				return func(info trace.ScriptingStreamExecuteDoneInfo) {
					if info.Error != nil {
						start.Error(info.Error)
					}
					start.End()
				}
			}
		}

		return nil
	}
	t.OnExplain = func(info trace.ScriptingExplainStartInfo) func(trace.ScriptingExplainDoneInfo) {
		if adapter.Details()&trace.ScriptingEvents != 0 {
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
				kv.String("query", info.Query),
			)

			return func(info trace.ScriptingExplainDoneInfo) {
				finish(start, info.Error)
			}
		}

		return nil
	}
	t.OnClose = func(info trace.ScriptingCloseStartInfo) func(trace.ScriptingCloseDoneInfo) {
		if adapter.Details()&trace.ScriptingEvents != 0 {
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
			)

			return func(info trace.ScriptingCloseDoneInfo) {
				finish(start, info.Error)
			}
		}

		return nil
	}

	return t
}
