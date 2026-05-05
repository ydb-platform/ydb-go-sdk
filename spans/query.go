package spans

import (
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/kv"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// query produces the QueryService spans following OTel semantic conventions.
//
// User-facing span names:
//   - ydb.CreateSession (CLIENT) — QueryService session creation
//     (CreateSession + first AttachStream message)
//   - ydb.ExecuteQuery  (CLIENT) — single ExecuteQuery RPC, including reading
//     the response stream from start to end
//   - ydb.Commit        (CLIENT) — CommitTransaction RPC
//   - ydb.Rollback      (CLIENT) — RollbackTransaction RPC
//
// All client-kind spans are augmented with the OTel database semantic
// attributes (db.system.name, db.namespace, server.address, server.port,
// network.peer.address, network.peer.port) by the adapter implementation;
// only the ydb.node.id custom tag is attached at this level.
//
//nolint:funlen,gocyclo
func query(adapter Adapter) trace.Query {
	return trace.Query{
		OnNew: func(info trace.QueryNewStartInfo) func(info trace.QueryNewDoneInfo) {
			if adapter.Details()&trace.QueryEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
			)

			return func(info trace.QueryNewDoneInfo) {
				start.End()
			}
		},
		OnClose: func(info trace.QueryCloseStartInfo) func(info trace.QueryCloseDoneInfo) {
			if adapter.Details()&trace.QueryEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
			)

			return func(info trace.QueryCloseDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnPoolNew: func(info trace.QueryPoolNewStartInfo) func(trace.QueryPoolNewDoneInfo) {
			if adapter.Details()&trace.QueryPoolEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
			)

			return func(info trace.QueryPoolNewDoneInfo) {
				start.End(
					kv.Int("Limit", info.Limit),
				)
			}
		},
		OnPoolClose: func(info trace.QueryPoolCloseStartInfo) func(trace.QueryPoolCloseDoneInfo) {
			if adapter.Details()&trace.QueryPoolEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
			)

			return func(info trace.QueryPoolCloseDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnPoolTry: func(info trace.QueryPoolTryStartInfo) func(trace.QueryPoolTryDoneInfo) {
			if adapter.Details()&trace.QueryPoolEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
			)

			return func(info trace.QueryPoolTryDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnPoolWith: func(info trace.QueryPoolWithStartInfo) func(trace.QueryPoolWithDoneInfo) {
			if adapter.Details()&trace.QueryPoolEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
			)

			return func(info trace.QueryPoolWithDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnPoolPut: func(info trace.QueryPoolPutStartInfo) func(trace.QueryPoolPutDoneInfo) {
			if adapter.Details()&trace.QueryPoolEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
			)

			return func(info trace.QueryPoolPutDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnPoolGet: func(info trace.QueryPoolGetStartInfo) func(trace.QueryPoolGetDoneInfo) {
			if adapter.Details()&trace.QueryPoolEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
			)

			return func(info trace.QueryPoolGetDoneInfo) {
				finish(
					start,
					info.Error,
					kv.Int("attempts", info.Attempts),
					kv.String("status", safeStatus(info.Session)),
					kv.String("node_id", safeNodeID(info.Session)),
					kv.String("session_id", safeID(info.Session)),
				)
			}
		},
		OnDo: func(info trace.QueryDoStartInfo) func(trace.QueryDoDoneInfo) {
			if adapter.Details()&trace.QueryEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
			)

			return func(info trace.QueryDoDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnDoTx: func(info trace.QueryDoTxStartInfo) func(trace.QueryDoTxDoneInfo) {
			if adapter.Details()&trace.QueryEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
			)

			return func(info trace.QueryDoTxDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnExec: func(info trace.QueryExecStartInfo) func(info trace.QueryExecDoneInfo) {
			if adapter.Details()&trace.QueryEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				SpanNameExecuteQuery,
			)

			return func(info trace.QueryExecDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnQuery: func(info trace.QueryQueryStartInfo) func(info trace.QueryQueryDoneInfo) {
			if adapter.Details()&trace.QueryEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				SpanNameExecuteQuery,
			)

			return func(info trace.QueryQueryDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnQueryResultSet: func(info trace.QueryQueryResultSetStartInfo) func(info trace.QueryQueryResultSetDoneInfo) {
			if adapter.Details()&trace.QueryEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				SpanNameExecuteQuery,
			)

			return func(info trace.QueryQueryResultSetDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnQueryRow: func(info trace.QueryQueryRowStartInfo) func(info trace.QueryQueryRowDoneInfo) {
			if adapter.Details()&trace.QueryEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				SpanNameExecuteQuery,
			)

			return func(info trace.QueryQueryRowDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnSessionCreate: func(info trace.QuerySessionCreateStartInfo) func(info trace.QuerySessionCreateDoneInfo) {
			if adapter.Details()&trace.QuerySessionEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				SpanNameCreateSession,
			)

			return func(info trace.QuerySessionCreateDoneInfo) {
				finish(start, info.Error,
					kv.Int64(AttrYDBNodeID, safeNodeIDInt64(info.Session)),
				)
			}
		},
		OnSessionAttach: func(info trace.QuerySessionAttachStartInfo) func(info trace.QuerySessionAttachDoneInfo) {
			if adapter.Details()&trace.QuerySessionEvents == 0 {
				return nil
			}

			ctx := *info.Context
			call := info.Call.String()

			return func(info trace.QuerySessionAttachDoneInfo) {
				if info.Error == nil {
					logToParentSpan(adapter, ctx, call)
				} else if xerrors.Is(info.Error, io.EOF) {
					logToParentSpan(adapter, ctx, call+" => io.EOF")
				} else {
					logToParentSpanError(adapter, ctx, info.Error)
				}
			}
		},
		OnSessionDelete: func(info trace.QuerySessionDeleteStartInfo) func(info trace.QuerySessionDeleteDoneInfo) {
			if adapter.Details()&trace.QuerySessionEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
				kv.Int64(AttrYDBNodeID, safeNodeIDInt64(info.Session)),
			)

			return func(info trace.QuerySessionDeleteDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnSessionExec: func(info trace.QuerySessionExecStartInfo) func(info trace.QuerySessionExecDoneInfo) {
			if adapter.Details()&trace.QuerySessionEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				SpanNameExecuteQuery,
				kv.Int64(AttrYDBNodeID, safeNodeIDInt64(info.Session)),
			)

			return func(info trace.QuerySessionExecDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnSessionQuery: func(info trace.QuerySessionQueryStartInfo) func(info trace.QuerySessionQueryDoneInfo) {
			if adapter.Details()&trace.QuerySessionEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				SpanNameExecuteQuery,
				kv.Int64(AttrYDBNodeID, safeNodeIDInt64(info.Session)),
			)

			return func(info trace.QuerySessionQueryDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnSessionQueryResultSet: func(
			info trace.QuerySessionQueryResultSetStartInfo,
		) func(info trace.QuerySessionQueryResultSetDoneInfo) {
			if adapter.Details()&trace.QuerySessionEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				SpanNameExecuteQuery,
				kv.Int64(AttrYDBNodeID, safeNodeIDInt64(info.Session)),
			)

			return func(info trace.QuerySessionQueryResultSetDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnSessionQueryRow: func(info trace.QuerySessionQueryRowStartInfo) func(info trace.QuerySessionQueryRowDoneInfo) {
			if adapter.Details()&trace.QuerySessionEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				SpanNameExecuteQuery,
				kv.Int64(AttrYDBNodeID, safeNodeIDInt64(info.Session)),
			)

			return func(info trace.QuerySessionQueryRowDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnSessionBegin: func(info trace.QuerySessionBeginStartInfo) func(info trace.QuerySessionBeginDoneInfo) {
			if adapter.Details()&trace.QuerySessionEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
				kv.Int64(AttrYDBNodeID, safeNodeIDInt64(info.Session)),
			)

			return func(info trace.QuerySessionBeginDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnTxCommit: func(info trace.QueryTxCommitStartInfo) func(info trace.QueryTxCommitDoneInfo) {
			if adapter.Details()&trace.QueryTransactionEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				SpanNameCommit,
				kv.Int64(AttrYDBNodeID, safeNodeIDInt64(info.Session)),
			)

			return func(info trace.QueryTxCommitDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnTxRollback: func(info trace.QueryTxRollbackStartInfo) func(info trace.QueryTxRollbackDoneInfo) {
			if adapter.Details()&trace.QueryTransactionEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				SpanNameRollback,
				kv.Int64(AttrYDBNodeID, safeNodeIDInt64(info.Session)),
			)

			return func(info trace.QueryTxRollbackDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnResultNew: func(info trace.QueryResultNewStartInfo) func(info trace.QueryResultNewDoneInfo) {
			if adapter.Details()&trace.QueryResultEvents == 0 {
				return nil
			}

			ctx := *info.Context
			call := info.Call.String()

			return func(info trace.QueryResultNewDoneInfo) {
				if info.Error == nil {
					logToParentSpan(adapter, ctx, call)
				} else {
					logToParentSpanError(adapter, ctx, info.Error)
				}
			}
		},
		OnResultNextPart: func(info trace.QueryResultNextPartStartInfo) func(info trace.QueryResultNextPartDoneInfo) {
			if adapter.Details()&trace.QueryResultEvents == 0 {
				return nil
			}

			ctx := *info.Context
			call := info.Call.String()

			return func(info trace.QueryResultNextPartDoneInfo) {
				if info.Error == nil {
					logToParentSpan(adapter, ctx, call)
				} else if xerrors.Is(info.Error, io.EOF) {
					logToParentSpan(adapter, ctx, call+" => io.EOF")
				} else {
					logToParentSpanError(adapter, ctx, info.Error)
				}
			}
		},
		OnResultNextResultSet: func(info trace.QueryResultNextResultSetStartInfo) func(
			info trace.QueryResultNextResultSetDoneInfo) {
			if adapter.Details()&trace.QueryResultEvents == 0 {
				return nil
			}

			ctx := *info.Context
			call := info.Call.String()

			return func(info trace.QueryResultNextResultSetDoneInfo) {
				if info.Error == nil {
					logToParentSpan(adapter, ctx, call)
				} else if xerrors.Is(info.Error, io.EOF) {
					logToParentSpan(adapter, ctx, call+" => io.EOF")
				} else {
					logToParentSpanError(adapter, ctx, info.Error)
				}
			}
		},
		OnResultClose: func(info trace.QueryResultCloseStartInfo) func(info trace.QueryResultCloseDoneInfo) {
			if adapter.Details()&trace.QueryResultEvents == 0 {
				return nil
			}

			ctx := *info.Context
			call := info.Call.String()

			return func(info trace.QueryResultCloseDoneInfo) {
				if info.Error == nil {
					logToParentSpan(adapter, ctx, call)
				} else {
					logToParentSpanError(adapter, ctx, info.Error)
				}
			}
		},
		OnTxExec: func(info trace.QueryTxExecStartInfo) func(info trace.QueryTxExecDoneInfo) {
			if adapter.Details()&trace.QueryTransactionEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				SpanNameExecuteQuery,
				kv.Int64(AttrYDBNodeID, safeNodeIDInt64(info.Session)),
				kv.Bool("WithCommit", info.WithCommit),
			)

			return func(info trace.QueryTxExecDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnTxQuery: func(info trace.QueryTxQueryStartInfo) func(info trace.QueryTxQueryDoneInfo) {
			if adapter.Details()&trace.QueryTransactionEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				SpanNameExecuteQuery,
				kv.Int64(AttrYDBNodeID, safeNodeIDInt64(info.Session)),
				kv.Bool("WithCommit", info.WithCommit),
			)

			return func(info trace.QueryTxQueryDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnTxQueryResultSet: func(info trace.QueryTxQueryResultSetStartInfo) func(info trace.QueryTxQueryResultSetDoneInfo) {
			if adapter.Details()&trace.QueryTransactionEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				SpanNameExecuteQuery,
				kv.Bool("WithCommit", info.WithCommit),
			)

			return func(info trace.QueryTxQueryResultSetDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnTxQueryRow: func(info trace.QueryTxQueryRowStartInfo) func(info trace.QueryTxQueryRowDoneInfo) {
			if adapter.Details()&trace.QueryTransactionEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				SpanNameExecuteQuery,
				kv.Bool("WithCommit", info.WithCommit),
			)

			return func(info trace.QueryTxQueryRowDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
	}
}
