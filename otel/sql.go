package otel

import (
	"context"

	otelTrace "go.opentelemetry.io/otel/trace"

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
func databaseSQL(cfg Config) (t trace.DatabaseSQL) {
	childSpanWithReplaceCtx := func(
		ctx *context.Context,
		operationName string,
		fields ...kv.KeyValue,
	) (s Span) {
		return childSpanWithReplaceCtx(cfg, ctx, operationName, fields...)
	}
	t.OnConnectorConnect = func(
		info trace.DatabaseSQLConnectorConnectStartInfo,
	) func(
		trace.DatabaseSQLConnectorConnectDoneInfo,
	) {
		if cfg.Details()&trace.DatabaseSQLConnectorEvents != 0 {
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
		if cfg.Details()&trace.DatabaseSQLConnEvents != 0 {
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
		if cfg.Details()&trace.DatabaseSQLConnEvents != 0 {
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
		if cfg.Details()&trace.DatabaseSQLConnEvents != 0 {
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
		if cfg.Details()&trace.DatabaseSQLConnEvents != 0 {
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
		if cfg.Details()&trace.DatabaseSQLConnEvents != 0 {
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
		if cfg.Details()&trace.DatabaseSQLTxEvents != 0 {
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
		if cfg.Details()&trace.DatabaseSQLTxEvents != 0 {
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
		if cfg.Details()&trace.DatabaseSQLTxEvents != 0 {
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
		if cfg.Details()&trace.DatabaseSQLTxEvents != 0 {
			if !isStmtCall(*info.Context) {
				*info.Context = otelTrace.ContextWithSpan(*info.Context, otelTrace.SpanFromContext(info.TxContext))
			}
			start := childSpanWithReplaceCtx(
				info.Context,
				info.Call.FunctionID(),
				kv.String("query", info.Query),
				kv.String("transaction_id", safeID(info.Tx)),
			)

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
		if cfg.Details()&trace.DatabaseSQLTxEvents != 0 {
			if !isStmtCall(*info.Context) {
				*info.Context = otelTrace.ContextWithSpan(*info.Context, otelTrace.SpanFromContext(info.TxContext))
			}
			start := childSpanWithReplaceCtx(
				info.Context,
				info.Call.FunctionID(),
				kv.String("query", info.Query),
				kv.String("transaction_id", safeID(info.Tx)),
			)

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
		if cfg.Details()&trace.DatabaseSQLTxEvents != 0 {
			*info.Context = otelTrace.ContextWithSpan(*info.Context, otelTrace.SpanFromContext(info.TxContext))
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
		if cfg.Details()&trace.DatabaseSQLStmtEvents != 0 {
			*info.Context = markStmtCall(
				otelTrace.ContextWithSpan(*info.Context, otelTrace.SpanFromContext(info.StmtContext)),
			)
			start := childSpanWithReplaceCtx(
				info.Context,
				info.Call.FunctionID(),
				kv.String("query", info.Query),
			)

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
		if cfg.Details()&trace.DatabaseSQLStmtEvents != 0 {
			*info.Context = markStmtCall(
				otelTrace.ContextWithSpan(*info.Context, otelTrace.SpanFromContext(info.StmtContext)),
			)
			start := childSpanWithReplaceCtx(
				info.Context,
				info.Call.FunctionID(),
				kv.String("query", info.Query),
			)

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
