package metrics

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// databaseSQL makes trace.DatabaseSQL with measuring `database/sql` events
func databaseSQL(config Config) (t trace.DatabaseSQL) {
	config = config.WithSystem("database").WithSystem("sql")
	conns := config.GaugeVec("conns")
	inflight := config.WithSystem("conns").GaugeVec("inflight")
	query := config.CounterVec("query", "status", "query_mode")
	queryLatency := config.WithSystem("query").TimerVec("latency", "query_mode")
	exec := config.CounterVec("exec", "status", "query_mode")
	execLatency := config.WithSystem("exec").TimerVec("latency", "query_mode")

	config = config.WithSystem("tx")
	txBegin := config.CounterVec("begin", "status")
	txBeginLatency := config.WithSystem("begin").TimerVec("latency")
	txExec := config.CounterVec("exec", "status")
	txExecLatency := config.WithSystem("exec").TimerVec("latency")
	txQuery := config.CounterVec("query", "status")
	txQueryLatency := config.WithSystem("query").TimerVec("latency")
	txCommit := config.CounterVec("commit", "status")
	txCommitLatency := config.WithSystem("commit").TimerVec("latency")
	txRollback := config.CounterVec("rollback", "status")
	txRollbackLatency := config.WithSystem("rollback").TimerVec("latency")
	t.OnConnectorConnect = func(info trace.DatabaseSQLConnectorConnectStartInfo) func(
		trace.DatabaseSQLConnectorConnectDoneInfo,
	) {
		if config.Details()&trace.DatabaseSQLConnectorEvents != 0 {
			return func(info trace.DatabaseSQLConnectorConnectDoneInfo) {
				if info.Error == nil {
					conns.With(nil).Add(1)
				}
			}
		}
		return nil
	}
	t.OnConnClose = func(info trace.DatabaseSQLConnCloseStartInfo) func(trace.DatabaseSQLConnCloseDoneInfo) {
		if config.Details()&trace.DatabaseSQLConnectorEvents != 0 {
			return func(info trace.DatabaseSQLConnCloseDoneInfo) {
				conns.With(nil).Add(-1)
			}
		}
		return nil
	}
	t.OnConnBegin = func(info trace.DatabaseSQLConnBeginStartInfo) func(trace.DatabaseSQLConnBeginDoneInfo) {
		start := time.Now()
		if config.Details()&trace.DatabaseSQLTxEvents != 0 {
			return func(info trace.DatabaseSQLConnBeginDoneInfo) {
				txBegin.With(map[string]string{
					"status": errorBrief(info.Error),
				}).Inc()
				txBeginLatency.With(nil).Record(time.Since(start))
			}
		}
		return nil
	}
	t.OnTxCommit = func(info trace.DatabaseSQLTxCommitStartInfo) func(trace.DatabaseSQLTxCommitDoneInfo) {
		start := time.Now()
		return func(info trace.DatabaseSQLTxCommitDoneInfo) {
			if config.Details()&trace.DatabaseSQLTxEvents != 0 {
				txCommit.With(map[string]string{
					"status": errorBrief(info.Error),
				}).Inc()
				txCommitLatency.With(nil).Record(time.Since(start))
			}
		}
	}
	t.OnTxExec = func(info trace.DatabaseSQLTxExecStartInfo) func(trace.DatabaseSQLTxExecDoneInfo) {
		start := time.Now()
		return func(info trace.DatabaseSQLTxExecDoneInfo) {
			if config.Details()&trace.DatabaseSQLTxEvents != 0 {
				status := errorBrief(info.Error)
				txExec.With(map[string]string{
					"status": status,
				}).Inc()
				txExecLatency.With(nil).Record(time.Since(start))
			}
		}
	}
	t.OnTxQuery = func(info trace.DatabaseSQLTxQueryStartInfo) func(trace.DatabaseSQLTxQueryDoneInfo) {
		start := time.Now()
		return func(info trace.DatabaseSQLTxQueryDoneInfo) {
			if config.Details()&trace.DatabaseSQLTxEvents != 0 {
				status := errorBrief(info.Error)
				txQuery.With(map[string]string{
					"status": status,
				}).Inc()
				txQueryLatency.With(nil).Record(time.Since(start))
			}
		}
	}
	t.OnTxRollback = func(info trace.DatabaseSQLTxRollbackStartInfo) func(trace.DatabaseSQLTxRollbackDoneInfo) {
		start := time.Now()
		return func(info trace.DatabaseSQLTxRollbackDoneInfo) {
			if config.Details()&trace.DatabaseSQLTxEvents != 0 {
				txRollback.With(map[string]string{
					"status": errorBrief(info.Error),
				}).Inc()
				txRollbackLatency.With(nil).Record(time.Since(start))
			}
		}
	}
	t.OnConnExec = func(info trace.DatabaseSQLConnExecStartInfo) func(trace.DatabaseSQLConnExecDoneInfo) {
		if config.Details()&trace.DatabaseSQLEvents != 0 {
			inflight.With(nil).Add(1)
		}
		var (
			mode  = info.Mode
			start = time.Now()
		)
		return func(info trace.DatabaseSQLConnExecDoneInfo) {
			if config.Details()&trace.DatabaseSQLEvents != 0 {
				inflight.With(nil).Add(-1)
			}
			if config.Details()&trace.DatabaseSQLConnEvents != 0 {
				status := errorBrief(info.Error)
				exec.With(map[string]string{
					"status":     status,
					"query_mode": mode,
				}).Inc()
				execLatency.With(map[string]string{
					"query_mode": mode,
				}).Record(time.Since(start))
			}
		}
	}
	t.OnConnQuery = func(info trace.DatabaseSQLConnQueryStartInfo) func(trace.DatabaseSQLConnQueryDoneInfo) {
		if config.Details()&trace.DatabaseSQLEvents != 0 {
			inflight.With(nil).Add(1)
		}
		var (
			mode  = info.Mode
			start = time.Now()
		)
		return func(info trace.DatabaseSQLConnQueryDoneInfo) {
			if config.Details()&trace.DatabaseSQLEvents != 0 {
				inflight.With(nil).Add(-1)
			}
			if config.Details()&trace.DatabaseSQLConnEvents != 0 {
				status := errorBrief(info.Error)
				query.With(map[string]string{
					"status":     status,
					"query_mode": mode,
				}).Inc()
				queryLatency.With(map[string]string{
					"query_mode": mode,
				}).Record(time.Since(start))
			}
		}
	}
	return t
}
