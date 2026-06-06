package spans

import (
	"context"
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/kv"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// query produces the QueryService spans following OTel semantic conventions.
//
// User-facing span names:
//   - ydb.CreateSession    (CLIENT) — QueryService session creation
//     (CreateSession + first AttachStream message)
//   - ydb.ExecuteQuery     (CLIENT) — single ExecuteQuery RPC, including reading
//     the response stream from start to end
//   - ydb.BeginTransaction (CLIENT) — explicit BeginTransaction RPC (eager
//     `s.Begin` and `Tx.UnLazy`); lazy DoTx never emits it
//   - ydb.Commit           (CLIENT) — CommitTransaction RPC
//   - ydb.Rollback         (CLIENT) — RollbackTransaction RPC
//
// Attribute provenance for client-kind spans:
//
//   - db.system.name, db.namespace, server.address, server.port — attached by
//     the adapter implementation from the driver configuration (the SDK does
//     not have direct access to the driver Endpoint / Database at this layer).
//   - network.peer.address, network.peer.port, ydb.node.id, ydb.node.dc —
//     attached by the SDK once the gRPC layer selects a concrete endpoint for
//     the RPC. See annotateNetworkPeer in driver.go: both the innermost open
//     span (SpanFromContext) and the surrounding top-level CLIENT span
//     (clientSpanFromContext, registered via withClientSpan below) get the
//     same set of peer/node attributes so every ydb.* CLIENT span ends up
//     with ydb.node.id / ydb.node.dc.
//   - ydb.node.id is additionally attached at span start for every session-
//     bound handler (OnSession* / OnTx*) using the node id already known on
//     the session, so the value is present even before the gRPC peer is
//     chosen and survives retries that never reach the wire.
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
				safeCall(info.Call),
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
				safeCall(info.Call),
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
				safeCall(info.Call),
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
				safeCall(info.Call),
			)

			return func(info trace.QueryPoolCloseDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		// OnPoolTry / OnPoolWith / OnPoolPut are intentionally not wired:
		// they are pool-level bookkeeping that duplicate ydb.RunWithRetry
		// and add no user-relevant detail to the span tree.
		OnPoolGet: func(info trace.QueryPoolGetStartInfo) func(trace.QueryPoolGetDoneInfo) {
			if adapter.Details()&trace.QueryPoolEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				SpanNameGetSession,
			)

			return func(info trace.QueryPoolGetDoneInfo) {
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
						kv.String("node_id", safeNodeID(info.Session)),
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
		},
		// OnDo / OnDoTx are intentionally not wired: the ydb.RunWithRetry
		// span emitted by spans.Retry already covers the whole retry-driven
		// operation (Do / DoTx run inside retry.Retry).
		OnExec: func(info trace.QueryExecStartInfo) func(info trace.QueryExecDoneInfo) {
			if adapter.Details()&trace.QueryEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				SpanNameExecuteQuery,
			)
			withContextPtr(info.Context, func(c context.Context) context.Context { return withClientSpan(c, start) })

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
			withContextPtr(info.Context, func(c context.Context) context.Context { return withClientSpan(c, start) })

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
			withContextPtr(info.Context, func(c context.Context) context.Context { return withClientSpan(c, start) })

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
			withContextPtr(info.Context, func(c context.Context) context.Context { return withClientSpan(c, start) })

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
				switch {
				case info.Error != nil:
					finish(
						start,
						info.Error,
						kv.Int64(AttrYDBNodeID, 0),
					)
				case info.Session != nil:
					finish(
						start,
						nil,
						kv.Int64(AttrYDBNodeID, safeNodeIDInt64(info.Session)),
					)
				default:
					finish(
						start,
						nil,
						kv.Int64(AttrYDBNodeID, 0),
					)
				}
			}
		},
		OnSessionAttach: func(info trace.QuerySessionAttachStartInfo) func(info trace.QuerySessionAttachDoneInfo) {
			if adapter.Details()&trace.QuerySessionEvents == 0 {
				return nil
			}

			ctx := safeContextPtr(info.Context)
			call := safeCall(info.Call)

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
		// OnSessionDelete is not wired: session deletion is an
		// implementation detail of the pool, not part of the user-facing
		// ydb.* span surface.
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
				safeCall(info.Call),
				kv.Int64(AttrYDBNodeID, safeNodeIDInt64(info.Session)),
			)

			return func(info trace.QuerySessionBeginDoneInfo) {
				switch {
				case info.Error != nil:
					finish(start, info.Error)
				case info.Tx != nil:
					finish(
						start,
						nil,
						kv.String("TransactionID", safeID(info.Tx)),
					)
				default:
					finish(start, nil)
				}
			}
		},
		// OnSessionBeginTransaction covers an actual gRPC BeginTransaction
		// RPC (eager `s.Begin` and `Tx.UnLazy`); lazy DoTx never fires this
		// event because the begin is fused into the first ExecuteQuery.
		OnSessionBeginTransaction: func(
			info trace.QuerySessionBeginTransactionStartInfo,
		) func(info trace.QuerySessionBeginTransactionDoneInfo) {
			if adapter.Details()&trace.QuerySessionEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				SpanNameBeginTransaction,
				kv.Int64(AttrYDBNodeID, safeNodeIDInt64(info.Session)),
			)

			return func(info trace.QuerySessionBeginTransactionDoneInfo) {
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

			ctx := safeContextPtr(info.Context)
			call := safeCall(info.Call)

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

			ctx := safeContextPtr(info.Context)
			call := safeCall(info.Call)

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

			ctx := safeContextPtr(info.Context)
			call := safeCall(info.Call)

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

			ctx := safeContextPtr(info.Context)
			call := safeCall(info.Call)

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
				kv.Int64(AttrYDBNodeID, safeNodeIDInt64(info.Session)),
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
				kv.Int64(AttrYDBNodeID, safeNodeIDInt64(info.Session)),
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
