package spans

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/kv"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type ctxStmtCallKey struct{}

func markStmtCall(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxStmtCallKey{}, true)
}

func isStmtCall(ctx context.Context) bool {
	if txStmt, has := ctx.Value(ctxStmtCallKey{}).(bool); has {
		return txStmt
	}

	return false
}

// databaseSQL makes trace.DatabaseSQL with logging events from details
//
//nolint:funlen
func databaseSQL(adapter Adapter) trace.DatabaseSQL {
	childSpanWithReplaceCtx := func(
		ctx *context.Context,
		operationName string,
		fields ...kv.KeyValue,
	) (s Span) {
		return childSpanWithReplaceCtx(adapter, ctx, operationName, fields...)
	}

	return trace.DatabaseSQL{
		OnConnectorConnect: func(
			info trace.DatabaseSQLConnectorConnectStartInfo,
		) func(
			trace.DatabaseSQLConnectorConnectDoneInfo,
		) {
			if adapter.Details()&trace.DatabaseSQLConnectorEvents == 0 {
				return nil
			}

			start := childSpanWithReplaceCtx(info.Context,
				safeCall(info.Call),
			)

			return func(info trace.DatabaseSQLConnectorConnectDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnConnPing: func(info trace.DatabaseSQLConnPingStartInfo) func(trace.DatabaseSQLConnPingDoneInfo) {
			if adapter.Details()&trace.DatabaseSQLConnEvents == 0 {
				return nil
			}

			start := childSpanWithReplaceCtx(
				info.Context,
				safeCall(info.Call),
			)

			return func(info trace.DatabaseSQLConnPingDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnConnPrepare: func(info trace.DatabaseSQLConnPrepareStartInfo) func(trace.DatabaseSQLConnPrepareDoneInfo) {
			if adapter.Details()&trace.DatabaseSQLConnEvents == 0 {
				return nil
			}

			start := childSpanWithReplaceCtx(
				info.Context,
				safeCall(info.Call),
				kv.String("query", info.Query),
			)

			return func(info trace.DatabaseSQLConnPrepareDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnConnExec: func(info trace.DatabaseSQLConnExecStartInfo) func(trace.DatabaseSQLConnExecDoneInfo) {
			if adapter.Details()&trace.DatabaseSQLConnEvents == 0 {
				return nil
			}

			start := childSpanWithReplaceCtx(
				info.Context,
				safeCall(info.Call),
				kv.String("query", info.Query),
				kv.String("query_mode", info.Mode),
				kv.Bool("idempotent", info.Idempotent),
			)

			return func(info trace.DatabaseSQLConnExecDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnConnQuery: func(info trace.DatabaseSQLConnQueryStartInfo) func(trace.DatabaseSQLConnQueryDoneInfo) {
			if adapter.Details()&trace.DatabaseSQLConnEvents == 0 {
				return nil
			}

			start := childSpanWithReplaceCtx(
				info.Context,
				safeCall(info.Call),
				kv.String("query", info.Query),
				kv.String("query_mode", info.Mode),
				kv.Bool("idempotent", info.Idempotent),
			)

			return func(info trace.DatabaseSQLConnQueryDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnConnIsTableExists: func(info trace.DatabaseSQLConnIsTableExistsStartInfo) func(
			trace.DatabaseSQLConnIsTableExistsDoneInfo,
		) {
			if adapter.Details()&trace.DatabaseSQLConnEvents == 0 {
				return nil
			}

			start := childSpanWithReplaceCtx(
				info.Context,
				safeCall(info.Call),
				kv.String("table_name", info.TableName),
			)

			return func(info trace.DatabaseSQLConnIsTableExistsDoneInfo) {
				finish(
					start,
					info.Error,
					kv.Bool("exists", info.Exists),
				)
			}
		},
		OnConnBegin: func(info trace.DatabaseSQLConnBeginStartInfo) func(trace.DatabaseSQLConnBeginDoneInfo) {
			if adapter.Details()&trace.DatabaseSQLTxEvents == 0 {
				return nil
			}

			start := childSpanWithReplaceCtx(
				info.Context,
				safeCall(info.Call),
			)

			return func(info trace.DatabaseSQLConnBeginDoneInfo) {
				if info.Error != nil {
					finish(
						start,
						info.Error,
					)
				} else if !isNil(info.Tx) {
					finish(
						start,
						nil,
						kv.String("transaction_id", safeID(info.Tx)),
					)
				} else {
					finish(start, nil)
				}
			}
		},
		OnConnBeginTx: func(info trace.DatabaseSQLConnBeginTxStartInfo) func(trace.DatabaseSQLConnBeginTxDoneInfo) {
			if adapter.Details()&trace.DatabaseSQLTxEvents == 0 {
				return nil
			}

			start := childSpanWithReplaceCtx(
				info.Context,
				safeCall(info.Call),
			)

			return func(info trace.DatabaseSQLConnBeginTxDoneInfo) {
				if info.Error != nil {
					finish(
						start,
						info.Error,
					)
				} else if !isNil(info.Tx) {
					finish(
						start,
						nil,
						kv.String("transaction_id", safeID(info.Tx)),
					)
				} else {
					finish(start, nil)
				}
			}
		},
		OnTxRollback: func(info trace.DatabaseSQLTxRollbackStartInfo) func(trace.DatabaseSQLTxRollbackDoneInfo) {
			if adapter.Details()&trace.DatabaseSQLTxEvents == 0 {
				return nil
			}

			start := childSpanWithReplaceCtx(
				info.Context,
				safeCall(info.Call),
				kv.String("transaction_id", safeID(info.Tx)),
			)

			return func(info trace.DatabaseSQLTxRollbackDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnTxCommit: func(info trace.DatabaseSQLTxCommitStartInfo) func(trace.DatabaseSQLTxCommitDoneInfo) {
			if adapter.Details()&trace.DatabaseSQLTxEvents == 0 {
				return nil
			}

			start := childSpanWithReplaceCtx(
				info.Context,
				safeCall(info.Call),
				kv.String("transaction_id", safeID(info.Tx)),
			)

			return func(info trace.DatabaseSQLTxCommitDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnTxExec: func(info trace.DatabaseSQLTxExecStartInfo) func(trace.DatabaseSQLTxExecDoneInfo) {
			if adapter.Details()&trace.DatabaseSQLTxEvents == 0 {
				return nil
			}

			start := childSpanWithReplaceCtx(
				info.Context,
				safeCall(info.Call),
				kv.String("query", info.Query),
				kv.String("transaction_id", safeID(info.Tx)),
			)

			if !isStmtCall(safeContextPtr(info.Context)) {
				start.Link(adapter.SpanFromContext(info.TxContext))
			}

			return func(info trace.DatabaseSQLTxExecDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnTxQuery: func(info trace.DatabaseSQLTxQueryStartInfo) func(trace.DatabaseSQLTxQueryDoneInfo) {
			if adapter.Details()&trace.DatabaseSQLTxEvents == 0 {
				return nil
			}

			start := childSpanWithReplaceCtx(
				info.Context,
				safeCall(info.Call),
				kv.String("query", info.Query),
				kv.String("transaction_id", safeID(info.Tx)),
			)

			if !isStmtCall(safeContextPtr(info.Context)) {
				start.Link(adapter.SpanFromContext(info.TxContext))
			}

			return func(info trace.DatabaseSQLTxQueryDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnTxPrepare: func(info trace.DatabaseSQLTxPrepareStartInfo) func(trace.DatabaseSQLTxPrepareDoneInfo) {
			if adapter.Details()&trace.DatabaseSQLTxEvents == 0 {
				return nil
			}

			start := childSpanWithReplaceCtx(
				info.Context,
				safeCall(info.Call),
				kv.String("query", info.Query),
				kv.String("transaction_id", safeID(info.Tx)),
			)

			return func(info trace.DatabaseSQLTxPrepareDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnStmtExec: func(info trace.DatabaseSQLStmtExecStartInfo) func(trace.DatabaseSQLStmtExecDoneInfo) {
			if adapter.Details()&trace.DatabaseSQLStmtEvents == 0 {
				return nil
			}

			start := childSpanWithReplaceCtx(
				info.Context,
				safeCall(info.Call),
				kv.String("query", info.Query),
			)

			start.Link(adapter.SpanFromContext(safeContext(info.StmtContext)))

			withContextPtr(info.Context, markStmtCall)

			return func(info trace.DatabaseSQLStmtExecDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnStmtQuery: func(info trace.DatabaseSQLStmtQueryStartInfo) func(trace.DatabaseSQLStmtQueryDoneInfo) {
			if adapter.Details()&trace.DatabaseSQLStmtEvents == 0 {
				return nil
			}

			start := childSpanWithReplaceCtx(
				info.Context,
				safeCall(info.Call),
				kv.String("query", info.Query),
			)

			start.Link(adapter.SpanFromContext(safeContext(info.StmtContext)))

			withContextPtr(info.Context, markStmtCall)

			return func(info trace.DatabaseSQLStmtQueryDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnConnClose: func(info trace.DatabaseSQLConnCloseStartInfo) func(trace.DatabaseSQLConnCloseDoneInfo) {
			if adapter.Details()&trace.DatabaseSQLStmtEvents == 0 {
				return nil
			}

			start := childSpanWithReplaceCtx(
				info.Context,
				safeCall(info.Call),
			)

			withContextPtr(info.Context, markStmtCall)

			return func(info trace.DatabaseSQLConnCloseDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnConnCheckNamedValue: func(info trace.DatabaseSQLConnCheckNamedValueStartInfo) func(
			trace.DatabaseSQLConnCheckNamedValueDoneInfo,
		) {
			if adapter.Details()&trace.DatabaseSQLStmtEvents == 0 {
				return nil
			}

			var (
				start = time.Now()
				ctx   = safeContextPtr(info.Context)
				call  = info.Call
				value = info.Value
			)

			return func(info trace.DatabaseSQLConnCheckNamedValueDoneInfo) {
				if info.Error != nil {
					logToParentSpanError(adapter, ctx, info.Error,
						kv.Any("value", value),
						kv.Latency(start),
					)
				} else {
					logToParentSpan(adapter, ctx, safeCall(call),
						kv.Any("value", value),
						kv.Latency(start),
					)
				}
			}
		},
		OnConnIsColumnExists: func(info trace.DatabaseSQLConnIsColumnExistsStartInfo) func(
			trace.DatabaseSQLConnIsColumnExistsDoneInfo,
		) {
			if adapter.Details()&trace.DatabaseSQLStmtEvents == 0 {
				return nil
			}

			start := childSpanWithReplaceCtx(
				info.Context,
				safeCall(info.Call),
			)

			return func(info trace.DatabaseSQLConnIsColumnExistsDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnConnGetIndexColumns: func(info trace.DatabaseSQLConnGetIndexColumnsStartInfo) func(
			trace.DatabaseSQLConnGetIndexColumnsDoneInfo,
		) {
			if adapter.Details()&trace.DatabaseSQLStmtEvents == 0 {
				return nil
			}

			start := childSpanWithReplaceCtx(
				info.Context,
				safeCall(info.Call),
			)

			return func(info trace.DatabaseSQLConnGetIndexColumnsDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnStmtClose: func(info trace.DatabaseSQLStmtCloseStartInfo) func(trace.DatabaseSQLStmtCloseDoneInfo) {
			if adapter.Details()&trace.DatabaseSQLStmtEvents == 0 {
				return nil
			}

			parentSpan := adapter.SpanFromContext(safeContextPtr(info.StmtContext))
			start := childSpanWithReplaceCtx(
				info.StmtContext,
				safeCall(info.Call),
			)

			start.Link(parentSpan)

			withContextPtr(info.StmtContext, markStmtCall)

			return func(info trace.DatabaseSQLStmtCloseDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnDoTx: func(info trace.DatabaseSQLDoTxStartInfo) func(trace.DatabaseSQLDoTxIntermediateInfo) func(
			trace.DatabaseSQLDoTxDoneInfo,
		) {
			if adapter.Details()&trace.DatabaseSQLStmtEvents == 0 {
				return nil
			}

			start := childSpanWithReplaceCtx(
				info.Context,
				safeCall(info.Call),
			)

			return func(info trace.DatabaseSQLDoTxIntermediateInfo) func(trace.DatabaseSQLDoTxDoneInfo) {
				return func(info trace.DatabaseSQLDoTxDoneInfo) {
					finish(
						start,
						info.Error,
					)
				}
			}
		},
	}
}
