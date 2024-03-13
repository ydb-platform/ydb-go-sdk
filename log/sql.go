package log

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// DatabaseSQL makes trace.DatabaseSQL with logging events from details
func DatabaseSQL(l Logger, d trace.Detailer, opts ...Option) (t trace.DatabaseSQL) {
	return internalDatabaseSQL(wrapLogger(l, opts...), d)
}

func internalDatabaseSQL(l *wrapper, d trace.Detailer) (t trace.DatabaseSQL) {
	logger := l.logger
	loggerQuery := l.logQuery

	t.OnConnectorConnect = connectorConnect(logger, d)
	t.OnConnPing = connPing(logger, d)
	t.OnConnClose = connClose(logger, d)
	t.OnConnBegin = connBegin(logger, d)
	t.OnConnPrepare = connPrepare(logger, loggerQuery, d)
	t.OnConnExec = connExec(logger, loggerQuery, d)
	t.OnConnQuery = connQuery(logger, loggerQuery, d)
	t.OnTxCommit = txCommit(logger, d)
	t.OnTxRollback = txRollback(logger, d)
	t.OnStmtClose = stmtClose(logger, d)
	t.OnStmtExec = stmtExec(logger, loggerQuery, d)
	t.OnStmtQuery = stmtQuery(logger, loggerQuery, d)

	return t
}

func connectorConnect(
	l Logger,
	d trace.Detailer,
) func(info trace.DatabaseSQLConnectorConnectStartInfo) func(trace.DatabaseSQLConnectorConnectDoneInfo) {
	return func(
		info trace.DatabaseSQLConnectorConnectStartInfo,
	) func(
		trace.DatabaseSQLConnectorConnectDoneInfo,
	) {
		if d.Details()&trace.DatabaseSQLConnectorEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "database", "sql", "connector", "connect")
		l.Log(ctx, "start")
		start := time.Now()

		return func(info trace.DatabaseSQLConnectorConnectDoneInfo) {
			if info.Error == nil {
				l.Log(WithLevel(ctx, DEBUG), "connected",
					latencyField(start),
					String("session_id", info.Session.ID()),
					String("session_status", info.Session.Status()),
				)
			} else {
				l.Log(WithLevel(ctx, ERROR), "failed",
					Error(info.Error),
					latencyField(start),
					versionField(),
				)
			}
		}
	}
}

func connPing(
	l Logger,
	d trace.Detailer,
) func(info trace.DatabaseSQLConnPingStartInfo) func(trace.DatabaseSQLConnPingDoneInfo) {
	return func(info trace.DatabaseSQLConnPingStartInfo) func(trace.DatabaseSQLConnPingDoneInfo) {
		if d.Details()&trace.DatabaseSQLConnEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "database", "sql", "conn", "ping")
		l.Log(ctx, "start")
		start := time.Now()

		return func(info trace.DatabaseSQLConnPingDoneInfo) {
			if info.Error == nil {
				l.Log(ctx, "done",
					latencyField(start),
				)
			} else {
				l.Log(WithLevel(ctx, ERROR), "failed",
					Error(info.Error),
					latencyField(start),
					versionField(),
				)
			}
		}
	}
}

func connClose(
	l Logger,
	d trace.Detailer,
) func(info trace.DatabaseSQLConnCloseStartInfo) func(trace.DatabaseSQLConnCloseDoneInfo) {
	return func(info trace.DatabaseSQLConnCloseStartInfo) func(trace.DatabaseSQLConnCloseDoneInfo) {
		if d.Details()&trace.DatabaseSQLConnEvents == 0 {
			return nil
		}
		ctx := with(context.Background(), TRACE, "ydb", "database", "sql", "conn", "close")
		l.Log(ctx, "start")
		start := time.Now()

		return func(info trace.DatabaseSQLConnCloseDoneInfo) {
			if info.Error == nil {
				l.Log(ctx, "done",
					latencyField(start),
				)
			} else {
				l.Log(WithLevel(ctx, WARN), "failed",
					Error(info.Error),
					latencyField(start),
					versionField(),
				)
			}
		}
	}
}

func connBegin(
	l Logger,
	d trace.Detailer,
) func(info trace.DatabaseSQLConnBeginStartInfo) func(trace.DatabaseSQLConnBeginDoneInfo) {
	return func(info trace.DatabaseSQLConnBeginStartInfo) func(trace.DatabaseSQLConnBeginDoneInfo) {
		if d.Details()&trace.DatabaseSQLConnEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "database", "sql", "conn", "begin", "tx")
		l.Log(ctx, "start")
		start := time.Now()

		return func(info trace.DatabaseSQLConnBeginDoneInfo) {
			if info.Error == nil {
				l.Log(ctx, "done",
					latencyField(start),
				)
			} else {
				l.Log(WithLevel(ctx, ERROR), "failed",
					Error(info.Error),
					latencyField(start),
					versionField(),
				)
			}
		}
	}
}

func connPrepare(
	l Logger,
	loggerQuery bool,
	d trace.Detailer,
) func(info trace.DatabaseSQLConnPrepareStartInfo) func(trace.DatabaseSQLConnPrepareDoneInfo) {
	return func(info trace.DatabaseSQLConnPrepareStartInfo) func(trace.DatabaseSQLConnPrepareDoneInfo) {
		if d.Details()&trace.DatabaseSQLConnEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "database", "sql", "conn", "prepare", "stmt")
		l.Log(ctx, "start",
			appendFieldByCondition(loggerQuery,
				String("query", info.Query),
			)...,
		)
		query := info.Query
		start := time.Now()

		return func(info trace.DatabaseSQLConnPrepareDoneInfo) {
			if info.Error == nil {
				l.Log(ctx, "done",
					latencyField(start),
				)
			} else {
				l.Log(WithLevel(ctx, ERROR), "failed",
					appendFieldByCondition(loggerQuery,
						String("query", query),
						Error(info.Error),
						latencyField(start),
						versionField(),
					)...,
				)
			}
		}
	}
}

func connExec(
	l Logger,
	loggerQuery bool,
	d trace.Detailer,
) func(info trace.DatabaseSQLConnExecStartInfo) func(trace.DatabaseSQLConnExecDoneInfo) {
	return func(info trace.DatabaseSQLConnExecStartInfo) func(trace.DatabaseSQLConnExecDoneInfo) {
		if d.Details()&trace.DatabaseSQLConnEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "database", "sql", "conn", "exec")
		l.Log(ctx, "start",
			appendFieldByCondition(loggerQuery,
				String("query", info.Query),
			)...,
		)
		query := info.Query
		idempotent := info.Idempotent
		start := time.Now()

		return func(info trace.DatabaseSQLConnExecDoneInfo) {
			if info.Error == nil {
				l.Log(ctx, "done",
					latencyField(start),
				)
			} else {
				m := retry.Check(info.Error)
				l.Log(WithLevel(ctx, ERROR), "failed",
					appendFieldByCondition(loggerQuery,
						String("query", query),
						Bool("retryable", m.MustRetry(idempotent)),
						Int64("code", m.StatusCode()),
						Bool("deleteSession", m.MustDeleteSession()),
						Error(info.Error),
						latencyField(start),
						versionField(),
					)...,
				)
			}
		}
	}
}

func connQuery(
	l Logger,
	loggerQuery bool,
	d trace.Detailer,
) func(info trace.DatabaseSQLConnQueryStartInfo) func(trace.DatabaseSQLConnQueryDoneInfo) {
	return func(info trace.DatabaseSQLConnQueryStartInfo) func(trace.DatabaseSQLConnQueryDoneInfo) {
		if d.Details()&trace.DatabaseSQLConnEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "database", "sql", "conn", "query")
		l.Log(ctx, "start",
			appendFieldByCondition(loggerQuery,
				String("query", info.Query),
			)...,
		)
		query := info.Query
		idempotent := info.Idempotent
		start := time.Now()

		return func(info trace.DatabaseSQLConnQueryDoneInfo) {
			if info.Error == nil {
				l.Log(ctx, "done",
					latencyField(start),
				)
			} else {
				m := retry.Check(info.Error)
				l.Log(WithLevel(ctx, ERROR), "failed",
					appendFieldByCondition(loggerQuery,
						String("query", query),
						Bool("retryable", m.MustRetry(idempotent)),
						Int64("code", m.StatusCode()),
						Bool("deleteSession", m.MustDeleteSession()),
						Error(info.Error),
						latencyField(start),
						versionField(),
					)...,
				)
			}
		}
	}
}

func txCommit(
	l Logger,
	d trace.Detailer,
) func(info trace.DatabaseSQLTxCommitStartInfo) func(trace.DatabaseSQLTxCommitDoneInfo) {
	return func(info trace.DatabaseSQLTxCommitStartInfo) func(trace.DatabaseSQLTxCommitDoneInfo) {
		if d.Details()&trace.DatabaseSQLTxEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "database", "sql", "tx", "commit")
		l.Log(ctx, "start")
		start := time.Now()

		return func(info trace.DatabaseSQLTxCommitDoneInfo) {
			if info.Error == nil {
				l.Log(ctx, "committed",
					latencyField(start),
				)
			} else {
				l.Log(WithLevel(ctx, ERROR), "failed",
					Error(info.Error),
					latencyField(start),
					versionField(),
				)
			}
		}
	}
}

func txRollback(
	l Logger,
	d trace.Detailer,
) func(info trace.DatabaseSQLTxRollbackStartInfo) func(trace.DatabaseSQLTxRollbackDoneInfo) {
	return func(info trace.DatabaseSQLTxRollbackStartInfo) func(trace.DatabaseSQLTxRollbackDoneInfo) {
		if d.Details()&trace.DatabaseSQLTxEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "database", "sql", "tx", "rollback")
		l.Log(ctx, "start")
		start := time.Now()

		return func(info trace.DatabaseSQLTxRollbackDoneInfo) {
			if info.Error == nil {
				l.Log(ctx, "done",
					latencyField(start),
				)
			} else {
				l.Log(WithLevel(ctx, WARN), "failed",
					Error(info.Error),
					latencyField(start),
					versionField(),
				)
			}
		}
	}
}

func stmtClose(
	l Logger,
	d trace.Detailer,
) func(info trace.DatabaseSQLStmtCloseStartInfo) func(trace.DatabaseSQLStmtCloseDoneInfo) {
	return func(info trace.DatabaseSQLStmtCloseStartInfo) func(trace.DatabaseSQLStmtCloseDoneInfo) {
		if d.Details()&trace.DatabaseSQLStmtEvents == 0 {
			return nil
		}
		ctx := with(context.Background(), TRACE, "ydb", "database", "sql", "stmt", "close")
		l.Log(ctx, "start")
		start := time.Now()

		return func(info trace.DatabaseSQLStmtCloseDoneInfo) {
			if info.Error == nil {
				l.Log(ctx, "closed",
					latencyField(start),
				)
			} else {
				l.Log(WithLevel(ctx, ERROR), "close failed",
					Error(info.Error),
					latencyField(start),
					versionField(),
				)
			}
		}
	}
}

func stmtExec(
	l Logger,
	loggerQuery bool,
	d trace.Detailer,
) func(info trace.DatabaseSQLStmtExecStartInfo) func(trace.DatabaseSQLStmtExecDoneInfo) {
	return func(info trace.DatabaseSQLStmtExecStartInfo) func(trace.DatabaseSQLStmtExecDoneInfo) {
		if d.Details()&trace.DatabaseSQLStmtEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "database", "sql", "stmt", "exec")
		l.Log(ctx, "start",
			appendFieldByCondition(loggerQuery,
				String("query", info.Query),
			)...,
		)
		query := info.Query
		start := time.Now()

		return func(info trace.DatabaseSQLStmtExecDoneInfo) {
			if info.Error == nil {
				l.Log(ctx, "done",
					Error(info.Error),
					latencyField(start),
				)
			} else {
				l.Log(WithLevel(ctx, ERROR), "failed",
					appendFieldByCondition(loggerQuery,
						String("query", query),
						Error(info.Error),
						latencyField(start),
						versionField(),
					)...,
				)
			}
		}
	}
}

func stmtQuery(
	l Logger,
	loggerQuery bool,
	d trace.Detailer,
) func(info trace.DatabaseSQLStmtQueryStartInfo) func(trace.DatabaseSQLStmtQueryDoneInfo) {
	return func(info trace.DatabaseSQLStmtQueryStartInfo) func(trace.DatabaseSQLStmtQueryDoneInfo) {
		if d.Details()&trace.DatabaseSQLStmtEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "database", "sql", "stmt", "query")
		l.Log(ctx, "start",
			appendFieldByCondition(loggerQuery,
				String("query", info.Query),
			)...,
		)
		query := info.Query
		start := time.Now()

		return func(info trace.DatabaseSQLStmtQueryDoneInfo) {
			if info.Error == nil {
				l.Log(ctx, "done",
					latencyField(start),
				)
			} else {
				l.Log(WithLevel(ctx, ERROR), "failed",
					appendFieldByCondition(loggerQuery,
						String("query", query),
						Error(info.Error),
						latencyField(start),
						versionField(),
					)...,
				)
			}
		}
	}
}
