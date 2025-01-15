package metrics

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// DatabaseSQL makes trace.DatabaseSQL with measuring `database/sql` events
//
//nolint:funlen
func DatabaseSQL(config Config) trace.DatabaseSQL {
	config = config.WithSystem("database").WithSystem("sql")

	conns := config.GaugeVec("conns")

	query := config.CounterVec("query", "status", "query_mode")
	queryLatency := config.WithSystem("query").TimerVec("latency", "query_mode")

	exec := config.CounterVec("exec", "status", "query_mode")
	execLatency := config.WithSystem("exec").TimerVec("latency", "query_mode")

	txs := config.GaugeVec("tx")
	txLatency := config.WithSystem("tx").TimerVec("latency")
	txStart := xsync.Map[string, time.Time]{}

	return trace.DatabaseSQL{
		OnConnectorConnect: func(info trace.DatabaseSQLConnectorConnectStartInfo) func(
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
		},
		OnConnClose: func(info trace.DatabaseSQLConnCloseStartInfo) func(trace.DatabaseSQLConnCloseDoneInfo) {
			if config.Details()&trace.DatabaseSQLConnectorEvents != 0 {
				return func(info trace.DatabaseSQLConnCloseDoneInfo) {
					conns.With(nil).Add(-1)
				}
			}

			return nil
		},
		OnConnBegin: func(info trace.DatabaseSQLConnBeginStartInfo) func(trace.DatabaseSQLConnBeginDoneInfo) {
			if config.Details()&trace.DatabaseSQLTxEvents != 0 {
				return func(info trace.DatabaseSQLConnBeginDoneInfo) {
					if info.Error == nil {
						txs.With(nil).Add(1)
						txStart.Set(info.Tx.ID(), time.Now())
					}
				}
			}

			return nil
		},
		OnConnBeginTx: func(info trace.DatabaseSQLConnBeginTxStartInfo) func(trace.DatabaseSQLConnBeginTxDoneInfo) {
			if config.Details()&trace.DatabaseSQLTxEvents != 0 {
				return func(info trace.DatabaseSQLConnBeginTxDoneInfo) {
					if info.Error == nil {
						txs.With(nil).Add(1)
						txStart.Set(info.Tx.ID(), time.Now())
					}
				}
			}

			return nil
		},
		OnTxCommit: func(info trace.DatabaseSQLTxCommitStartInfo) func(trace.DatabaseSQLTxCommitDoneInfo) {
			txs.With(nil).Add(-1)

			if start, has := txStart.Extract(info.Tx.ID()); has {
				txLatency.With(nil).Record(time.Since(start))
			}

			return nil
		},
		OnTxRollback: func(info trace.DatabaseSQLTxRollbackStartInfo) func(trace.DatabaseSQLTxRollbackDoneInfo) {
			txs.With(nil).Add(-1)

			if start, has := txStart.Extract(info.Tx.ID()); has {
				txLatency.With(nil).Record(time.Since(start))
			}

			return nil
		},
		OnConnExec: func(info trace.DatabaseSQLConnExecStartInfo) func(trace.DatabaseSQLConnExecDoneInfo) {
			var (
				mode  = info.Mode
				start = time.Now()
			)

			return func(info trace.DatabaseSQLConnExecDoneInfo) {
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
		},
		OnConnQuery: func(info trace.DatabaseSQLConnQueryStartInfo) func(trace.DatabaseSQLConnQueryDoneInfo) {
			var (
				mode  = info.Mode
				start = time.Now()
			)

			return func(info trace.DatabaseSQLConnQueryDoneInfo) {
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
		},
		OnConnPing:            nil,
		OnConnPrepare:         nil,
		OnConnCheckNamedValue: nil,
		OnConnIsTableExists:   nil,
		OnConnIsColumnExists:  nil,
		OnConnGetIndexColumns: nil,
		OnTxExec:              nil,
		OnTxQuery:             nil,
		OnTxPrepare:           nil,
		OnStmtQuery:           nil,
		OnStmtExec:            nil,
		OnStmtClose:           nil,
		OnDoTx:                nil,
	}
}
