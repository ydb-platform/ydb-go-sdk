package log

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// DatabaseSQL makes trace.DatabaseSQL with logging events from details
func DatabaseSQL(l Logger, d trace.Detailer, opts ...option) (t trace.DatabaseSQL) {
	options := parseOptions(opts...)
	t.OnConnectorConnect = func(
		info trace.DatabaseSQLConnectorConnectStartInfo,
	) func(
		trace.DatabaseSQLConnectorConnectDoneInfo,
	) {
		if d.Details()&trace.DatabaseSQLConnectorEvents == 0 {
			return nil
		}
		ll := l.WithNames("database", "sql", "connector", "connect")
		ll.Log(TRACE, "start")
		start := time.Now()
		return func(info trace.DatabaseSQLConnectorConnectDoneInfo) {
			if info.Error == nil {
				ll.Log(INFO, "connected",
					latency(start),
				)
			} else {
				ll.Log(ERROR, "failed",
					Error(info.Error),
					latency(start),
					version(),
				)
			}
		}
	}

	t.OnConnPing = func(info trace.DatabaseSQLConnPingStartInfo) func(trace.DatabaseSQLConnPingDoneInfo) {
		if d.Details()&trace.DatabaseSQLConnEvents == 0 {
			return nil
		}
		ll := l.WithNames("database", "sql", "conn", "ping")
		ll.Log(TRACE, "start")
		start := time.Now()
		return func(info trace.DatabaseSQLConnPingDoneInfo) {
			if info.Error == nil {
				ll.Log(DEBUG, "done",
					latency(start),
				)
			} else {
				ll.Log(ERROR, "failed",
					Error(info.Error),
					latency(start),
					version(),
				)
			}
		}
	}
	t.OnConnClose = func(info trace.DatabaseSQLConnCloseStartInfo) func(trace.DatabaseSQLConnCloseDoneInfo) {
		if d.Details()&trace.DatabaseSQLConnEvents == 0 {
			return nil
		}
		ll := l.WithNames("database", "sql", "conn", "close")
		ll.Log(TRACE, "start")
		start := time.Now()
		return func(info trace.DatabaseSQLConnCloseDoneInfo) {
			if info.Error == nil {
				ll.Log(INFO, "done",
					latency(start),
				)
			} else {
				ll.Log(ERROR, "failed",
					Error(info.Error),
					latency(start),
					version(),
				)
			}
		}
	}
	t.OnConnBegin = func(info trace.DatabaseSQLConnBeginStartInfo) func(trace.DatabaseSQLConnBeginDoneInfo) {
		if d.Details()&trace.DatabaseSQLConnEvents == 0 {
			return nil
		}
		ll := l.WithNames("database", "sql", "conn", "begin", "tx")
		ll.Log(TRACE, "start")
		start := time.Now()
		return func(info trace.DatabaseSQLConnBeginDoneInfo) {
			if info.Error == nil {
				ll.Log(DEBUG, "done",
					latency(start),
				)
			} else {
				ll.Log(ERROR, "failed",
					Error(info.Error),
					latency(start),
					version(),
				)
			}
		}
	}
	t.OnConnPrepare = func(info trace.DatabaseSQLConnPrepareStartInfo) func(trace.DatabaseSQLConnPrepareDoneInfo) {
		if d.Details()&trace.DatabaseSQLConnEvents == 0 {
			return nil
		}
		ll := l.WithNames("database", "sql", "conn", "prepare", "stmt")
		ll.Log(TRACE, "start",
			appendFieldByCondition(options.logQuery,
				String("query", info.Query),
			)...,
		)
		query := info.Query
		start := time.Now()
		return func(info trace.DatabaseSQLConnPrepareDoneInfo) {
			if info.Error == nil {
				ll.Log(DEBUG, "done",
					latency(start),
				)
			} else {
				ll.Log(ERROR, "failed",
					appendFieldByCondition(options.logQuery,
						String("query", query),
						Error(info.Error),
						latency(start),
						version(),
					)...,
				)
			}
		}
	}
	t.OnConnExec = func(info trace.DatabaseSQLConnExecStartInfo) func(trace.DatabaseSQLConnExecDoneInfo) {
		if d.Details()&trace.DatabaseSQLConnEvents == 0 {
			return nil
		}
		ll := l.WithNames("database", "sql", "conn", "exec")
		ll.Log(TRACE, "start",
			appendFieldByCondition(options.logQuery,
				String("query", info.Query),
			)...,
		)
		query := info.Query
		idempotent := info.Idempotent
		start := time.Now()
		return func(info trace.DatabaseSQLConnExecDoneInfo) {
			if info.Error == nil {
				ll.Log(DEBUG, "done",
					latency(start),
				)
			} else {
				m := retry.Check(info.Error)
				ll.Log(ERROR, "failed",
					appendFieldByCondition(options.logQuery,
						String("query", query),
						Bool("retryable", m.MustRetry(idempotent)),
						Int64("code", m.StatusCode()),
						Bool("deleteSession", m.MustDeleteSession()),
						Error(info.Error),
						latency(start),
						version(),
					)...,
				)
			}
		}
	}
	t.OnConnQuery = func(info trace.DatabaseSQLConnQueryStartInfo) func(trace.DatabaseSQLConnQueryDoneInfo) {
		if d.Details()&trace.DatabaseSQLConnEvents == 0 {
			return nil
		}
		ll := l.WithNames("database", "sql", "conn", "query")
		ll.Log(TRACE, "start",
			appendFieldByCondition(options.logQuery,
				String("query", info.Query),
			)...,
		)
		query := info.Query
		idempotent := info.Idempotent
		start := time.Now()
		return func(info trace.DatabaseSQLConnQueryDoneInfo) {
			if info.Error == nil {
				ll.Log(DEBUG, "done",
					latency(start),
				)
			} else {
				m := retry.Check(info.Error)
				ll.Log(ERROR, "failed",
					appendFieldByCondition(options.logQuery,
						String("query", query),
						Bool("retryable", m.MustRetry(idempotent)),
						Int64("code", m.StatusCode()),
						Bool("deleteSession", m.MustDeleteSession()),
						Error(info.Error),
						latency(start),
						version(),
					)...,
				)
			}
		}
	}
	t.OnTxCommit = func(info trace.DatabaseSQLTxCommitStartInfo) func(trace.DatabaseSQLTxCommitDoneInfo) {
		if d.Details()&trace.DatabaseSQLTxEvents == 0 {
			return nil
		}
		ll := l.WithNames("database", "sql", "tx", "commit")
		ll.Log(TRACE, "start")
		start := time.Now()
		return func(info trace.DatabaseSQLTxCommitDoneInfo) {
			if info.Error == nil {
				ll.Log(DEBUG, "committed",
					latency(start),
				)
			} else {
				ll.Log(ERROR, "failed",
					Error(info.Error),
					latency(start),
					version(),
				)
			}
		}
	}
	t.OnTxRollback = func(info trace.DatabaseSQLTxRollbackStartInfo) func(trace.DatabaseSQLTxRollbackDoneInfo) {
		if d.Details()&trace.DatabaseSQLTxEvents == 0 {
			return nil
		}
		ll := l.WithNames("database", "sql", "tx", "rollback")
		ll.Log(TRACE, "start")
		start := time.Now()
		return func(info trace.DatabaseSQLTxRollbackDoneInfo) {
			if info.Error == nil {
				ll.Log(DEBUG, "done",
					latency(start),
				)
			} else {
				ll.Log(ERROR, "failed",
					Error(info.Error),
					latency(start),
					version(),
				)
			}
		}
	}
	t.OnStmtClose = func(info trace.DatabaseSQLStmtCloseStartInfo) func(trace.DatabaseSQLStmtCloseDoneInfo) {
		if d.Details()&trace.DatabaseSQLStmtEvents == 0 {
			return nil
		}
		ll := l.WithNames("database", "sql", "stmt", "close")
		ll.Log(TRACE, "start")
		start := time.Now()
		return func(info trace.DatabaseSQLStmtCloseDoneInfo) {
			if info.Error == nil {
				ll.Log(TRACE, "closed",
					latency(start),
				)
			} else {
				ll.Log(ERROR, "close failed",
					Error(info.Error),
					latency(start),
					version(),
				)
			}
		}
	}
	t.OnStmtExec = func(info trace.DatabaseSQLStmtExecStartInfo) func(trace.DatabaseSQLStmtExecDoneInfo) {
		if d.Details()&trace.DatabaseSQLStmtEvents == 0 {
			return nil
		}
		ll := l.WithNames("database", "sql", "stmt", "exec")
		ll.Log(TRACE, "start",
			appendFieldByCondition(options.logQuery,
				String("query", info.Query),
			)...,
		)
		query := info.Query
		start := time.Now()
		return func(info trace.DatabaseSQLStmtExecDoneInfo) {
			if info.Error == nil {
				ll.Log(DEBUG, "done",
					Error(info.Error),
					latency(start),
				)
			} else {
				ll.Log(ERROR, "failed",
					appendFieldByCondition(options.logQuery,
						String("query", query),
						Error(info.Error),
						latency(start),
						version(),
					)...,
				)
			}
		}
	}
	t.OnStmtQuery = func(info trace.DatabaseSQLStmtQueryStartInfo) func(trace.DatabaseSQLStmtQueryDoneInfo) {
		if d.Details()&trace.DatabaseSQLStmtEvents == 0 {
			return nil
		}
		ll := l.WithNames("database", "sql", "stmt", "query")
		ll.Log(TRACE, "start",
			appendFieldByCondition(options.logQuery,
				String("query", info.Query),
			)...,
		)
		query := info.Query
		start := time.Now()
		return func(info trace.DatabaseSQLStmtQueryDoneInfo) {
			if info.Error == nil {
				ll.Log(DEBUG, "done",
					latency(start),
				)
			} else {
				ll.Log(ERROR, "failed",
					appendFieldByCondition(options.logQuery,
						String("query", query),
						Error(info.Error),
						latency(start),
						version(),
					)...,
				)
			}
		}
	}
	return t
}
