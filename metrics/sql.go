package metrics

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// databaseSQL makes trace.DatabaseSQL with measuring `database/sql` events
func databaseSQL(config Config) (t trace.DatabaseSQL) {
	config = config.WithSystem("database").WithSystem("sql")

	metrics := DatabaseMetrics{
		conns:             config.GaugeVec("conns"),
		inflight:          config.WithSystem("conns").GaugeVec("inflight"),
		query:             config.CounterVec("query", "status", "query_mode"),
		queryLatency:      config.WithSystem("query").TimerVec("latency", "query_mode"),
		exec:              config.CounterVec("exec", "status", "query_mode"),
		execLatency:       config.WithSystem("exec").TimerVec("latency", "query_mode"),
		txBegin:           config.WithSystem("tx").CounterVec("begin", "status"),
		txBeginLatency:    config.WithSystem("tx").TimerVec("latency"),
		txExec:            config.WithSystem("tx").CounterVec("exec", "status"),
		txExecLatency:     config.WithSystem("tx").TimerVec("latency"),
		txQuery:           config.WithSystem("tx").CounterVec("query", "status"),
		txQueryLatency:    config.WithSystem("tx").TimerVec("latency"),
		txCommit:          config.WithSystem("tx").CounterVec("commit", "status"),
		txCommitLatency:   config.WithSystem("tx").TimerVec("latency"),
		txRollback:        config.WithSystem("tx").CounterVec("rollback", "status"),
		txRollbackLatency: config.WithSystem("tx").TimerVec("latency"),
	}

	t = setupDatabaseEventHandlers(config, metrics)
	return t
}

func setupDatabaseEventHandlers(config Config, metrics DatabaseMetrics) trace.DatabaseSQL {
	var t trace.DatabaseSQL

	t.OnConnectorConnect = setupOnConnectorConnect(config, metrics.conns)
	t.OnConnClose = setupOnConnCloseConnect(config, metrics.conns)
	t.OnConnBegin = setupOnConnBegin(config, metrics.txBegin, metrics.txBeginLatency)
	t.OnTxCommit = setupOnTxCommit(config, metrics.txCommit, metrics.txCommitLatency)
	t.OnTxExec = setupOnTxExec(config, metrics.txExec, metrics.txExecLatency)
	t.OnTxQuery = setupOnTxQuery(config, metrics.txQuery, metrics.txQueryLatency)
	t.OnTxRollback = setupOnTxRollback(config, metrics.txRollback, metrics.txRollbackLatency)
	t.OnConnExec = setupOnConnExec(config, metrics.inflight, metrics.exec, metrics.execLatency)
	t.OnConnQuery = setupOnConnQuery(config, metrics.inflight, metrics.query, metrics.queryLatency)

	return t
}

func setupOnConnectorConnect(config Config, conns GaugeVec) func(trace.DatabaseSQLConnectorConnectStartInfo) func(trace.DatabaseSQLConnectorConnectDoneInfo) {
	return func(info trace.DatabaseSQLConnectorConnectStartInfo) func(trace.DatabaseSQLConnectorConnectDoneInfo) {
		if config.Details()&trace.DatabaseSQLConnectorEvents != 0 {
			return func(info trace.DatabaseSQLConnectorConnectDoneInfo) {
				if info.Error == nil {
					conns.With(nil).Add(1)
				}
			}
		}

		return nil
	}
}

func setupOnConnCloseConnect(config Config, conns GaugeVec) func(trace.DatabaseSQLConnCloseStartInfo) func(trace.DatabaseSQLConnCloseDoneInfo) {
	return func(info trace.DatabaseSQLConnCloseStartInfo) func(trace.DatabaseSQLConnCloseDoneInfo) {
		if config.Details()&trace.DatabaseSQLConnectorEvents != 0 {
			return func(info trace.DatabaseSQLConnCloseDoneInfo) {
				conns.With(nil).Add(-1)
			}
		}

		return nil
	}
}

func setupOnConnBegin(config Config, txBegin CounterVec, txBeginLatency TimerVec) func(trace.DatabaseSQLConnBeginStartInfo) func(trace.DatabaseSQLConnBeginDoneInfo) {
	return func(info trace.DatabaseSQLConnBeginStartInfo) func(trace.DatabaseSQLConnBeginDoneInfo) {
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
}

func setupOnTxCommit(config Config, txCommit CounterVec, txCommitLatency TimerVec) func(trace.DatabaseSQLTxCommitStartInfo) func(trace.DatabaseSQLTxCommitDoneInfo) {
	return func(info trace.DatabaseSQLTxCommitStartInfo) func(trace.DatabaseSQLTxCommitDoneInfo) {
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
}

func setupOnTxExec(config Config, txExec CounterVec, txExecLatency TimerVec) func(trace.DatabaseSQLTxExecStartInfo) func(trace.DatabaseSQLTxExecDoneInfo) {
	return func(info trace.DatabaseSQLTxExecStartInfo) func(trace.DatabaseSQLTxExecDoneInfo) {
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
}

func setupOnTxQuery(config Config, txQuery CounterVec, txQueryLatency TimerVec) func(trace.DatabaseSQLTxQueryStartInfo) func(trace.DatabaseSQLTxQueryDoneInfo) {
	return func(info trace.DatabaseSQLTxQueryStartInfo) func(trace.DatabaseSQLTxQueryDoneInfo) {
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
}

func setupOnTxRollback(config Config, txRollback CounterVec, txRollbackLatency TimerVec) func(trace.DatabaseSQLTxRollbackStartInfo) func(trace.DatabaseSQLTxRollbackDoneInfo) {
	return func(info trace.DatabaseSQLTxRollbackStartInfo) func(trace.DatabaseSQLTxRollbackDoneInfo) {
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
}

func setupOnConnExec(config Config, inflight GaugeVec, exec CounterVec, execLatency TimerVec) func(trace.DatabaseSQLConnExecStartInfo) func(trace.DatabaseSQLConnExecDoneInfo) {
	return func(info trace.DatabaseSQLConnExecStartInfo) func(trace.DatabaseSQLConnExecDoneInfo) {
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
}

func setupOnConnQuery(config Config, inflight GaugeVec, query CounterVec, queryLatency TimerVec) func(trace.DatabaseSQLConnQueryStartInfo) func(trace.DatabaseSQLConnQueryDoneInfo) {
	return func(info trace.DatabaseSQLConnQueryStartInfo) func(trace.DatabaseSQLConnQueryDoneInfo) {
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
}
