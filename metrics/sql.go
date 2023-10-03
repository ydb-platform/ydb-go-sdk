package metrics

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// databaseSQL makes trace.DatabaseSQL with measuring `database/sql` events
func databaseSQL(config Config) (t trace.DatabaseSQL) {
	config = config.WithSystem("database").WithSystem("sql")
	conns := config.GaugeVec("conns")
	txs := config.GaugeVec("txs")
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
		if config.Details()&trace.DatabaseSQLTxEvents != 0 {
			return func(info trace.DatabaseSQLConnBeginDoneInfo) {
				if info.Tx != nil {
					txs.With(nil).Add(1)
				}
			}
		}
		return nil
	}
	t.OnTxCommit = func(info trace.DatabaseSQLTxCommitStartInfo) func(trace.DatabaseSQLTxCommitDoneInfo) {
		if config.Details()&trace.DatabaseSQLTxEvents != 0 {
			return func(info trace.DatabaseSQLTxCommitDoneInfo) {
				if info.Error == nil {
					txs.With(nil).Add(-1)
				}
			}
		}
		return nil
	}
	t.OnTxRollback = func(info trace.DatabaseSQLTxRollbackStartInfo) func(trace.DatabaseSQLTxRollbackDoneInfo) {
		if config.Details()&trace.DatabaseSQLTxEvents != 0 {
			return func(info trace.DatabaseSQLTxRollbackDoneInfo) {
				if info.Error == nil {
					txs.With(nil).Add(-1)
				}
			}
		}
		return nil
	}
	return t
}
