package spans

import (
	"context"

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
func databaseSQL(adapter Adapter) (t trace.DatabaseSQL) {
	childSpanWithReplaceCtx := func(
		ctx *context.Context,
		operationName string,
		fields ...kv.KeyValue,
	) (s Span) {
		return childSpanWithReplaceCtx(adapter, ctx, operationName, fields...)
	}
	t.OnConnectorConnect = func(
		info trace.DatabaseSQLConnectorConnectStartInfo,
	) func(
		trace.DatabaseSQLConnectorConnectDoneInfo,
	) {
		if adapter.Details()&trace.DatabaseSQLConnectorEvents != 0 {
			start := childSpanWithReplaceCtx(info.Context,
				info.Call.FunctionID(),
			)

			return func(info trace.DatabaseSQLConnectorConnectDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}

		return nil
	}
	t.OnConnPing = func(info trace.DatabaseSQLConnPingStartInfo) func(trace.DatabaseSQLConnPingDoneInfo) {
		if adapter.Details()&trace.DatabaseSQLConnEvents != 0 {
			start := childSpanWithReplaceCtx(
				info.Context,
				info.Call.FunctionID(),
			)

			return func(info trace.DatabaseSQLConnPingDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}

		return nil
	}
	t.OnConnPrepare = func(info trace.DatabaseSQLConnPrepareStartInfo) func(trace.DatabaseSQLConnPrepareDoneInfo) {
		if adapter.Details()&trace.DatabaseSQLConnEvents != 0 {
			start := childSpanWithReplaceCtx(
				info.Context,
				info.Call.FunctionID(),
				kv.String("query", info.Query),
			)

			return func(info trace.DatabaseSQLConnPrepareDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}

		return nil
	}
	t.OnConnExec = func(info trace.DatabaseSQLConnExecStartInfo) func(trace.DatabaseSQLConnExecDoneInfo) {
		if adapter.Details()&trace.DatabaseSQLConnEvents != 0 {
			start := childSpanWithReplaceCtx(
				info.Context,
				info.Call.FunctionID(),
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
		}

		return nil
	}
	t.OnConnQuery = func(info trace.DatabaseSQLConnQueryStartInfo) func(trace.DatabaseSQLConnQueryDoneInfo) {
		if adapter.Details()&trace.DatabaseSQLConnEvents != 0 {
			start := childSpanWithReplaceCtx(
				info.Context,
				info.Call.FunctionID(),
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
		}

		return nil
	}
	t.OnConnIsTableExists = func(info trace.DatabaseSQLConnIsTableExistsStartInfo) func(
		trace.DatabaseSQLConnIsTableExistsDoneInfo,
	) {
		if adapter.Details()&trace.DatabaseSQLConnEvents != 0 {
			start := childSpanWithReplaceCtx(
				info.Context,
				info.Call.FunctionID(),
				kv.String("table_name", info.TableName),
			)

			return func(info trace.DatabaseSQLConnIsTableExistsDoneInfo) {
				finish(
					start,
					info.Error,
					kv.Bool("exists", info.Exists),
				)
			}
		}

		return nil
	}
	t.OnConnBegin = func(info trace.DatabaseSQLConnBeginStartInfo) func(trace.DatabaseSQLConnBeginDoneInfo) {
		if adapter.Details()&trace.DatabaseSQLTxEvents != 0 {
			start := childSpanWithReplaceCtx(
				info.Context,
				info.Call.FunctionID(),
			)

			return func(info trace.DatabaseSQLConnBeginDoneInfo) {
				finish(
					start,
					info.Error,
					kv.String("transaction_id", safeID(info.Tx)),
				)
			}
		}

		return nil
	}
	t.OnTxRollback = func(info trace.DatabaseSQLTxRollbackStartInfo) func(trace.DatabaseSQLTxRollbackDoneInfo) {
		if adapter.Details()&trace.DatabaseSQLTxEvents != 0 {
			start := childSpanWithReplaceCtx(
				info.Context,
				info.Call.FunctionID(),
				kv.String("transaction_id", safeID(info.Tx)),
			)

			return func(info trace.DatabaseSQLTxRollbackDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}

		return nil
	}
	t.OnTxCommit = func(info trace.DatabaseSQLTxCommitStartInfo) func(trace.DatabaseSQLTxCommitDoneInfo) {
		if adapter.Details()&trace.DatabaseSQLTxEvents != 0 {
			start := childSpanWithReplaceCtx(
				info.Context,
				info.Call.FunctionID(),
				kv.String("transaction_id", safeID(info.Tx)),
			)

			return func(info trace.DatabaseSQLTxCommitDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}

		return nil
	}
	t.OnTxExec = func(info trace.DatabaseSQLTxExecStartInfo) func(trace.DatabaseSQLTxExecDoneInfo) {
		if adapter.Details()&trace.DatabaseSQLTxEvents != 0 {
			start := childSpanWithReplaceCtx(
				info.Context,
				info.Call.FunctionID(),
				kv.String("query", info.Query),
				kv.String("transaction_id", safeID(info.Tx)),
			)

			if !isStmtCall(*info.Context) {
				start.Link(adapter.SpanFromContext(info.TxContext))
			}

			return func(info trace.DatabaseSQLTxExecDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}

		return nil
	}
	t.OnTxQuery = func(info trace.DatabaseSQLTxQueryStartInfo) func(trace.DatabaseSQLTxQueryDoneInfo) {
		if adapter.Details()&trace.DatabaseSQLTxEvents != 0 {
			start := childSpanWithReplaceCtx(
				info.Context,
				info.Call.FunctionID(),
				kv.String("query", info.Query),
				kv.String("transaction_id", safeID(info.Tx)),
			)

			if !isStmtCall(*info.Context) {
				start.Link(adapter.SpanFromContext(info.TxContext))
			}

			return func(info trace.DatabaseSQLTxQueryDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}

		return nil
	}
	t.OnTxPrepare = func(info trace.DatabaseSQLTxPrepareStartInfo) func(trace.DatabaseSQLTxPrepareDoneInfo) {
		if adapter.Details()&trace.DatabaseSQLTxEvents != 0 {
			start := childSpanWithReplaceCtx(
				info.Context,
				info.Call.FunctionID(),
				kv.String("query", info.Query),
				kv.String("transaction_id", safeID(info.Tx)),
			)

			return func(info trace.DatabaseSQLTxPrepareDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}

		return nil
	}
	t.OnStmtExec = func(info trace.DatabaseSQLStmtExecStartInfo) func(trace.DatabaseSQLStmtExecDoneInfo) {
		if adapter.Details()&trace.DatabaseSQLStmtEvents != 0 {
			start := childSpanWithReplaceCtx(
				info.Context,
				info.Call.FunctionID(),
				kv.String("query", info.Query),
			)

			start.Link(adapter.SpanFromContext(info.StmtContext))

			*info.Context = markStmtCall(*info.Context)

			return func(info trace.DatabaseSQLStmtExecDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}

		return nil
	}
	t.OnStmtQuery = func(info trace.DatabaseSQLStmtQueryStartInfo) func(trace.DatabaseSQLStmtQueryDoneInfo) {
		if adapter.Details()&trace.DatabaseSQLStmtEvents != 0 {
			start := childSpanWithReplaceCtx(
				info.Context,
				info.Call.FunctionID(),
				kv.String("query", info.Query),
			)

			start.Link(adapter.SpanFromContext(info.StmtContext))

			*info.Context = markStmtCall(*info.Context)

			return func(info trace.DatabaseSQLStmtQueryDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}

		return nil
	}

	return t
}
