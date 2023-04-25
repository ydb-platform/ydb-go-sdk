package log

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// DatabaseSQL makes trace.DatabaseSQL with logging events from details
func DatabaseSQL(l Logger, d trace.Detailer, opts ...Option) (t trace.DatabaseSQL) {
	return internalDatabaseSQL(wrapLogger(l, opts...), d)
}

func internalDatabaseSQL(l *wrapper, d trace.Detailer) (t trace.DatabaseSQL) {
	t.OnConnectorConnect = func(
		info trace.DatabaseSQLConnectorConnectStartInfo,
	) func(
		trace.DatabaseSQLConnectorConnectDoneInfo,
	) {
		if d.Details()&trace.DatabaseSQLConnectorEvents == 0 {
			return nil
		}
		params := Params{
			Ctx:       *info.Context,
			Level:     TRACE,
			Namespace: []string{"database", "sql", "connector", "connect"},
		}
		l.Log(params.withLevel(TRACE), "start")
		start := time.Now()
		return func(info trace.DatabaseSQLConnectorConnectDoneInfo) {
			if info.Error == nil {
				l.Log(params.withLevel(INFO), "connected",
					latency(start),
				)
			} else {
				l.Log(params.withLevel(ERROR), "failed",
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
		params := Params{
			Ctx:       *info.Context,
			Level:     TRACE,
			Namespace: []string{"database", "sql", "conn", "ping"},
		}
		l.Log(params.withLevel(TRACE), "start")
		start := time.Now()
		return func(info trace.DatabaseSQLConnPingDoneInfo) {
			if info.Error == nil {
				l.Log(params.withLevel(DEBUG), "done",
					latency(start),
				)
			} else {
				l.Log(params.withLevel(ERROR), "failed",
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
		params := Params{
			Level:     TRACE,
			Namespace: []string{"database", "sql", "conn", "close"},
		}
		l.Log(params.withLevel(TRACE), "start")
		start := time.Now()
		return func(info trace.DatabaseSQLConnCloseDoneInfo) {
			if info.Error == nil {
				l.Log(params.withLevel(INFO), "done",
					latency(start),
				)
			} else {
				l.Log(params.withLevel(ERROR), "failed",
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
		params := Params{
			Ctx:       *info.Context,
			Level:     TRACE,
			Namespace: []string{"database", "sql", "conn", "begin", "tx"},
		}
		l.Log(params.withLevel(TRACE), "start")
		start := time.Now()
		return func(info trace.DatabaseSQLConnBeginDoneInfo) {
			if info.Error == nil {
				l.Log(params.withLevel(DEBUG), "done",
					latency(start),
				)
			} else {
				l.Log(params.withLevel(ERROR), "failed",
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
		params := Params{
			Ctx:       *info.Context,
			Level:     TRACE,
			Namespace: []string{"database", "sql", "conn", "prepare", "stmt"},
		}
		l.Log(params.withLevel(TRACE), "start",
			appendFieldByCondition(l.logQuery,
				String("query", info.Query),
			)...,
		)
		query := info.Query
		start := time.Now()
		return func(info trace.DatabaseSQLConnPrepareDoneInfo) {
			if info.Error == nil {
				l.Log(params.withLevel(DEBUG), "done",
					latency(start),
				)
			} else {
				l.Log(params.withLevel(ERROR), "failed",
					appendFieldByCondition(l.logQuery,
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
		params := Params{
			Ctx:       *info.Context,
			Level:     TRACE,
			Namespace: []string{"database", "sql", "conn", "exec"},
		}
		l.Log(params.withLevel(TRACE), "start",
			appendFieldByCondition(l.logQuery,
				String("query", info.Query),
			)...,
		)
		query := info.Query
		idempotent := info.Idempotent
		start := time.Now()
		return func(info trace.DatabaseSQLConnExecDoneInfo) {
			if info.Error == nil {
				l.Log(params.withLevel(DEBUG), "done",
					latency(start),
				)
			} else {
				m := retry.Check(info.Error)
				l.Log(params.withLevel(ERROR), "failed",
					appendFieldByCondition(l.logQuery,
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
		params := Params{
			Ctx:       *info.Context,
			Level:     TRACE,
			Namespace: []string{"database", "sql", "conn", "query"},
		}
		l.Log(params.withLevel(TRACE), "start",
			appendFieldByCondition(l.logQuery,
				String("query", info.Query),
			)...,
		)
		query := info.Query
		idempotent := info.Idempotent
		start := time.Now()
		return func(info trace.DatabaseSQLConnQueryDoneInfo) {
			if info.Error == nil {
				l.Log(params.withLevel(DEBUG), "done",
					latency(start),
				)
			} else {
				m := retry.Check(info.Error)
				l.Log(params.withLevel(ERROR), "failed",
					appendFieldByCondition(l.logQuery,
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
		params := Params{
			Ctx:       *info.Context,
			Level:     TRACE,
			Namespace: []string{"database", "sql", "tx", "commit"},
		}
		l.Log(params.withLevel(TRACE), "start")
		start := time.Now()
		return func(info trace.DatabaseSQLTxCommitDoneInfo) {
			if info.Error == nil {
				l.Log(params.withLevel(DEBUG), "committed",
					latency(start),
				)
			} else {
				l.Log(params.withLevel(ERROR), "failed",
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
		params := Params{
			Ctx:       *info.Context,
			Level:     TRACE,
			Namespace: []string{"database", "sql", "tx", "rollback"},
		}
		l.Log(params.withLevel(TRACE), "start")
		start := time.Now()
		return func(info trace.DatabaseSQLTxRollbackDoneInfo) {
			if info.Error == nil {
				l.Log(params.withLevel(DEBUG), "done",
					latency(start),
				)
			} else {
				l.Log(params.withLevel(ERROR), "failed",
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
		params := Params{
			Level:     TRACE,
			Namespace: []string{"database", "sql", "stmt", "close"},
		}
		l.Log(params.withLevel(TRACE), "start")
		start := time.Now()
		return func(info trace.DatabaseSQLStmtCloseDoneInfo) {
			if info.Error == nil {
				l.Log(params.withLevel(TRACE), "closed",
					latency(start),
				)
			} else {
				l.Log(params.withLevel(ERROR), "close failed",
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
		params := Params{
			Ctx:       *info.Context,
			Level:     TRACE,
			Namespace: []string{"database", "sql", "stmt", "exec"},
		}
		l.Log(params.withLevel(TRACE), "start",
			appendFieldByCondition(l.logQuery,
				String("query", info.Query),
			)...,
		)
		query := info.Query
		start := time.Now()
		return func(info trace.DatabaseSQLStmtExecDoneInfo) {
			if info.Error == nil {
				l.Log(params.withLevel(DEBUG), "done",
					Error(info.Error),
					latency(start),
				)
			} else {
				l.Log(params.withLevel(ERROR), "failed",
					appendFieldByCondition(l.logQuery,
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
		params := Params{
			Ctx:       *info.Context,
			Level:     TRACE,
			Namespace: []string{"database", "sql", "stmt", "query"},
		}
		l.Log(params.withLevel(TRACE), "start",
			appendFieldByCondition(l.logQuery,
				String("query", info.Query),
			)...,
		)
		query := info.Query
		start := time.Now()
		return func(info trace.DatabaseSQLStmtQueryDoneInfo) {
			if info.Error == nil {
				l.Log(params.withLevel(DEBUG), "done",
					latency(start),
				)
			} else {
				l.Log(params.withLevel(ERROR), "failed",
					appendFieldByCondition(l.logQuery,
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
