package spans

import (
	"net/url"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/kv"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// table makes table.ClientTrace with solomon metrics publishing
//
//nolint:funlen
func table(adapter Adapter) (t trace.Table) { //nolint:gocyclo
	nodeID := func(sessionID string) string {
		u, err := url.Parse(sessionID)
		if err != nil {
			return ""
		}

		return u.Query().Get("node_id")
	}
	t.OnCreateSession = func(info trace.TableCreateSessionStartInfo) func(trace.TableCreateSessionDoneInfo) {
		if adapter.Details()&trace.TableEvents == 0 {
			return nil
		}

		fieldsStore := fieldsStoreFromContext(info.Context)
		*info.Context = withFunctionID(*info.Context, info.Call.String())

		return func(info trace.TableCreateSessionDoneInfo) {
			if info.Error == nil && info.Session != nil {
				fieldsStore.fields = append(fieldsStore.fields,
					kv.String("session_id", safeID(info.Session)),
					kv.String("session_status", safeStatus(info.Session)),
					kv.String("node_id", nodeID(safeID(info.Session))),
				)
			}
		}
	}
	t.OnDo = func(info trace.TableDoStartInfo) func(trace.TableDoDoneInfo) {
		if adapter.Details()&trace.TableEvents == 0 {
			return nil
		}

		*info.Context = noTraceRetry(*info.Context)
		operationName := info.Label
		if operationName == "" {
			operationName = info.Call.String()
		}
		start := childSpanWithReplaceCtx(
			adapter,
			info.Context,
			operationName,
			kv.Bool("idempotent", info.Idempotent),
		)
		if info.NestedCall {
			start.Warn(errNestedCall)
		}

		return func(info trace.TableDoDoneInfo) {
			fields := []KeyValue{
				kv.Int("attempts", info.Attempts),
			}
			if info.Error != nil {
				start.Error(info.Error)
			}
			start.End(fields...)
		}
	}
	t.OnDoTx = func(info trace.TableDoTxStartInfo) func(trace.TableDoTxDoneInfo) {
		if adapter.Details()&trace.TableEvents == 0 {
			return nil
		}

		*info.Context = noTraceRetry(*info.Context)
		operationName := info.Label
		if operationName == "" {
			operationName = info.Call.String()
		}
		start := childSpanWithReplaceCtx(
			adapter,
			info.Context,
			operationName,
			kv.Bool("idempotent", info.Idempotent),
		)
		if info.NestedCall {
			start.Warn(errNestedCall)
		}

		return func(info trace.TableDoTxDoneInfo) {
			fields := []KeyValue{
				kv.Int("attempts", info.Attempts),
			}
			if info.Error != nil {
				start.Error(info.Error)
			}
			start.End(fields...)
		}
	}
	t.OnSessionNew = func(info trace.TableSessionNewStartInfo) func(trace.TableSessionNewDoneInfo) {
		if adapter.Details()&trace.TableSessionLifeCycleEvents == 0 {
			return nil
		}

		start := childSpanWithReplaceCtx(
			adapter,
			info.Context,
			info.Call.String(),
		)

		return func(info trace.TableSessionNewDoneInfo) {
			if info.Error != nil {
				finish(
					start,
					info.Error,
				)
			} else if info.Session != nil {
				finish(
					start,
					nil,
					kv.String("status", safeStatus(info.Session)),
					kv.String("node_id", nodeID(safeID(info.Session))),
					kv.String("session_id", safeID(info.Session)),
				)
			} else {
				finish(start, nil)
			}
		}
	}
	t.OnSessionDelete = func(info trace.TableSessionDeleteStartInfo) func(trace.TableSessionDeleteDoneInfo) {
		if adapter.Details()&trace.TableSessionLifeCycleEvents == 0 {
			return nil
		}

		ctx := *info.Context
		call := info.Call.String()
		fields := []KeyValue{
			kv.String("node_id", nodeID(safeID(info.Session))),
			kv.String("session_id", safeID(info.Session)),
		}

		return func(info trace.TableSessionDeleteDoneInfo) {
			if info.Error == nil {
				logToParentSpan(adapter, ctx, call, fields...)
			} else {
				logToParentSpanError(adapter, ctx, info.Error, fields...)
			}
		}
	}
	t.OnSessionKeepAlive = func(info trace.TableKeepAliveStartInfo) func(trace.TableKeepAliveDoneInfo) {
		if adapter.Details()&trace.TableSessionLifeCycleEvents == 0 {
			return nil
		}

		start := childSpanWithReplaceCtx(
			adapter,
			info.Context,
			info.Call.String(),
			kv.String("node_id", nodeID(safeID(info.Session))),
			kv.String("session_id", safeID(info.Session)),
		)

		return func(info trace.TableKeepAliveDoneInfo) {
			finish(start, info.Error)
		}
	}
	t.OnSessionBulkUpsert = func(info trace.TableSessionBulkUpsertStartInfo) func(trace.TableSessionBulkUpsertDoneInfo) {
		if adapter.Details()&trace.TableSessionQueryEvents == 0 {
			return nil
		}

		start := childSpanWithReplaceCtx(
			adapter,
			info.Context,
			info.Call.String(),
			kv.String("node_id", nodeID(safeID(info.Session))),
			kv.String("session_id", safeID(info.Session)),
		)

		return func(info trace.TableSessionBulkUpsertDoneInfo) {
			finish(start, info.Error)
		}
	}
	t.OnSessionQueryPrepare = func(
		info trace.TablePrepareDataQueryStartInfo,
	) func(
		trace.TablePrepareDataQueryDoneInfo,
	) {
		if adapter.Details()&trace.TableSessionQueryInvokeEvents == 0 {
			return nil
		}

		start := childSpanWithReplaceCtx(
			adapter,
			info.Context,
			info.Call.String(),
			kv.String("query", info.Query),
			kv.String("node_id", nodeID(safeID(info.Session))),
			kv.String("session_id", safeID(info.Session)),
		)

		return func(info trace.TablePrepareDataQueryDoneInfo) {
			finish(
				start,
				info.Error,
				kv.String("result", safeStringer(info.Result)),
			)
		}
	}
	t.OnSessionQueryExecute = func(
		info trace.TableExecuteDataQueryStartInfo,
	) func(
		trace.TableExecuteDataQueryDoneInfo,
	) {
		if adapter.Details()&trace.TableSessionQueryInvokeEvents == 0 {
			return nil
		}

		start := childSpanWithReplaceCtx(
			adapter,
			info.Context,
			info.Call.String(),
			kv.String("node_id", nodeID(safeID(info.Session))),
			kv.String("session_id", safeID(info.Session)),
			kv.String("query", safeStringer(info.Query)),
			kv.Bool("keep_in_cache", info.KeepInCache),
		)

		return func(info trace.TableExecuteDataQueryDoneInfo) {
			if info.Error != nil {
				finish(
					start,
					info.Error,
				)
			} else if info.Tx != nil {
				finish(
					start,
					safeErr(info.Result),
					kv.Bool("prepared", info.Prepared),
					kv.String("transaction_id", safeID(info.Tx)),
				)
			} else {
				finish(
					start,
					safeErr(info.Result),
					kv.Bool("prepared", info.Prepared),
				)
			}
		}
	}
	t.OnSessionQueryStreamExecute = func(
		info trace.TableSessionQueryStreamExecuteStartInfo,
	) func(
		trace.TableSessionQueryStreamExecuteDoneInfo,
	) {
		if adapter.Details()&trace.TableSessionQueryStreamEvents == 0 {
			return nil
		}

		start := childSpanWithReplaceCtx(
			adapter,
			info.Context,
			info.Call.String(),
			kv.String("query", safeStringer(info.Query)),
			kv.String("node_id", nodeID(safeID(info.Session))),
			kv.String("session_id", safeID(info.Session)),
		)

		return func(info trace.TableSessionQueryStreamExecuteDoneInfo) {
			if info.Error != nil {
				start.Error(info.Error)
			}
			start.End()
		}
	}
	t.OnSessionQueryStreamRead = func(
		info trace.TableSessionQueryStreamReadStartInfo,
	) func(
		trace.TableSessionQueryStreamReadDoneInfo,
	) {
		if adapter.Details()&trace.TableSessionQueryStreamEvents == 0 {
			return nil
		}

		start := childSpanWithReplaceCtx(
			adapter,
			info.Context,
			info.Call.String(),
			kv.String("node_id", nodeID(safeID(info.Session))),
			kv.String("session_id", safeID(info.Session)),
		)

		return func(info trace.TableSessionQueryStreamReadDoneInfo) {
			if info.Error != nil {
				start.Error(info.Error)
			}
			start.End()
		}
	}
	t.OnTxBegin = func(info trace.TableTxBeginStartInfo) func(trace.TableTxBeginDoneInfo) {
		if adapter.Details()&trace.TableSessionTransactionEvents == 0 {
			return nil
		}

		start := childSpanWithReplaceCtx(
			adapter,
			info.Context,
			info.Call.String(),
			kv.String("node_id", nodeID(safeID(info.Session))),
			kv.String("session_id", safeID(info.Session)),
		)

		return func(info trace.TableTxBeginDoneInfo) {
			if info.Error != nil {
				finish(
					start,
					info.Error,
				)
			} else if info.Tx != nil {
				finish(
					start,
					nil,
					kv.String("transaction_id", safeID(info.Tx)),
				)
			} else {
				finish(start, nil)
			}
		}
	}
	t.OnTxCommit = func(info trace.TableTxCommitStartInfo) func(trace.TableTxCommitDoneInfo) {
		if adapter.Details()&trace.TableSessionTransactionEvents == 0 {
			return nil
		}

		start := childSpanWithReplaceCtx(
			adapter,
			info.Context,
			info.Call.String(),
			kv.String("node_id", nodeID(safeID(info.Session))),
			kv.String("session_id", safeID(info.Session)),
			kv.String("transaction_id", safeID(info.Tx)),
		)

		return func(info trace.TableTxCommitDoneInfo) {
			finish(start, info.Error)
		}
	}
	t.OnTxRollback = func(info trace.TableTxRollbackStartInfo) func(trace.TableTxRollbackDoneInfo) {
		if adapter.Details()&trace.TableSessionTransactionEvents == 0 {
			return nil
		}

		start := childSpanWithReplaceCtx(
			adapter,
			info.Context,
			info.Call.String(),
			kv.String("node_id", nodeID(safeID(info.Session))),
			kv.String("session_id", safeID(info.Session)),
			kv.String("transaction_id", safeID(info.Tx)),
		)

		return func(info trace.TableTxRollbackDoneInfo) {
			finish(start, info.Error)
		}
	}
	t.OnTxExecute = func(info trace.TableTransactionExecuteStartInfo) func(trace.TableTransactionExecuteDoneInfo) {
		if adapter.Details()&trace.TableSessionTransactionEvents == 0 {
			return nil
		}

		start := childSpanWithReplaceCtx(
			adapter,
			info.Context,
			info.Call.String(),
			kv.String("node_id", nodeID(safeID(info.Session))),
			kv.String("session_id", safeID(info.Session)),
			kv.String("transaction_id", safeID(info.Tx)),
			kv.String("query", safeStringer(info.Query)),
		)

		return func(info trace.TableTransactionExecuteDoneInfo) {
			finish(start, info.Error)
		}
	}
	t.OnTxExecuteStatement = func(
		info trace.TableTransactionExecuteStatementStartInfo,
	) func(
		info trace.TableTransactionExecuteStatementDoneInfo,
	) {
		if adapter.Details()&trace.TableSessionTransactionEvents == 0 {
			return nil
		}

		start := childSpanWithReplaceCtx(
			adapter,
			info.Context,
			info.Call.String(),
			kv.String("node_id", nodeID(safeID(info.Session))),
			kv.String("session_id", safeID(info.Session)),
			kv.String("transaction_id", safeID(info.Tx)),
			kv.String("query", safeStringer(info.StatementQuery)),
		)

		return func(info trace.TableTransactionExecuteStatementDoneInfo) {
			finish(start, info.Error)
		}
	}
	t.OnInit = func(info trace.TableInitStartInfo) func(trace.TableInitDoneInfo) {
		if adapter.Details()&trace.TableEvents == 0 {
			return nil
		}

		start := childSpanWithReplaceCtx(
			adapter,
			info.Context,
			info.Call.String(),
		)

		return func(info trace.TableInitDoneInfo) {
			finish(
				start,
				nil,
				kv.Int("limit", info.Limit),
			)
		}
	}
	t.OnClose = func(info trace.TableCloseStartInfo) func(trace.TableCloseDoneInfo) {
		if adapter.Details()&trace.TableEvents == 0 {
			return nil
		}

		start := childSpanWithReplaceCtx(
			adapter,
			info.Context,
			info.Call.String(),
		)

		return func(info trace.TableCloseDoneInfo) {
			finish(start, info.Error)
		}
	}
	t.OnPoolPut = func(info trace.TablePoolPutStartInfo) func(trace.TablePoolPutDoneInfo) {
		if adapter.Details()&trace.TablePoolAPIEvents == 0 {
			return nil
		}

		start := childSpanWithReplaceCtx(
			adapter,
			info.Context,
			info.Call.String(),
			kv.String("node_id", nodeID(safeID(info.Session))),
			kv.String("session_id", safeID(info.Session)),
		)

		return func(info trace.TablePoolPutDoneInfo) {
			finish(start, info.Error)
		}
	}
	t.OnPoolGet = func(info trace.TablePoolGetStartInfo) func(trace.TablePoolGetDoneInfo) {
		if adapter.Details()&trace.TablePoolAPIEvents == 0 {
			return nil
		}

		start := childSpanWithReplaceCtx(
			adapter,
			info.Context,
			info.Call.String(),
		)

		return func(info trace.TablePoolGetDoneInfo) {
			if info.Error != nil {
				finish(
					start,
					info.Error,
					kv.Int("attempts", info.Attempts),
				)
			} else if info.Session != nil {
				finish(
					start,
					nil,
					kv.Int("attempts", info.Attempts),
					kv.String("status", safeStatus(info.Session)),
					kv.String("node_id", nodeID(safeID(info.Session))),
					kv.String("session_id", safeID(info.Session)),
				)
			} else {
				finish(
					start,
					nil,
					kv.Int("attempts", info.Attempts),
				)
			}
		}
	}

	return t
}
