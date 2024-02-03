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

	databaseSQLConnectorEvents := config.Details() & trace.DatabaseSQLConnectorEvents
	databaseSQLTxEvents := config.Details() & trace.DatabaseSQLTxEvents
	databaseSQLEvents := config.Details() & trace.DatabaseSQLEvents
	databaseSQLConnEvents := config.Details() & trace.DatabaseSQLConnEvents

	t.OnConnectorConnect = onConnectorConnect(databaseSQLConnectorEvents, conns)
	t.OnConnClose = onConnClose(databaseSQLConnectorEvents, conns)
	t.OnConnBegin = onConnBegin(databaseSQLTxEvents, txBegin, txBeginLatency)
	t.OnTxCommit = onTxCommit(databaseSQLTxEvents, txCommit, txCommitLatency)
	t.OnTxExec = onTxExec(databaseSQLTxEvents, txExec, txExecLatency)
	t.OnTxQuery = onTxQuery(databaseSQLTxEvents, txQuery, txQueryLatency)
	t.OnTxRollback = onTxRollback(databaseSQLTxEvents, txRollback, txRollbackLatency)
	t.OnConnExec = onConnExec(databaseSQLEvents, databaseSQLConnEvents, inflight, exec, execLatency)
	t.OnConnQuery = onConnQuery(databaseSQLEvents, databaseSQLConnEvents, inflight, query, queryLatency)

	return t
}

// The `onConnectorConnect` function is called when a connection is established to the database.
func onConnectorConnect(
	databaseSQLConnectorEvents trace.Details,
	conns GaugeVec,
) func(info trace.DatabaseSQLConnectorConnectStartInfo) func(trace.DatabaseSQLConnectorConnectDoneInfo) {
	return func(info trace.DatabaseSQLConnectorConnectStartInfo) func(
		trace.DatabaseSQLConnectorConnectDoneInfo,
	) {
		if databaseSQLConnectorEvents != 0 {
			return func(info trace.DatabaseSQLConnectorConnectDoneInfo) {
				if info.Error == nil {
					conns.With(nil).Add(1)
				}
			}
		}

		return nil
	}
}

// onConnClose is a function that is used to handle the closing of a database connection.
func onConnClose(
	databaseSQLConnectorEvents trace.Details,
	conns GaugeVec,
) func(info trace.DatabaseSQLConnCloseStartInfo) func(trace.DatabaseSQLConnCloseDoneInfo) {
	return func(info trace.DatabaseSQLConnCloseStartInfo) func(trace.DatabaseSQLConnCloseDoneInfo) {
		if databaseSQLConnectorEvents != 0 {
			return func(info trace.DatabaseSQLConnCloseDoneInfo) {
				conns.With(nil).Add(-1)
			}
		}

		return nil
	}
}

// onConnBegin measures `trace.DatabaseSQLConnBeginStartInfo` events and updates `txBegin` and `txBeginLatency`
// counters. It returns a function that captures `trace.DatabaseSQLConnBeginDoneInfo` events and updates the
// counters accordingly.
func onConnBegin(
	databaseSQLTxEvents trace.Details,
	txBegin CounterVec,
	txBeginLatency TimerVec,
) func(info trace.DatabaseSQLConnBeginStartInfo) func(trace.DatabaseSQLConnBeginDoneInfo) {
	return func(info trace.DatabaseSQLConnBeginStartInfo) func(trace.DatabaseSQLConnBeginDoneInfo) {
		start := time.Now()
		if databaseSQLTxEvents != 0 {
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

// onTxCommit is a function that returns a closure function. The closure function
// takes a trace.DatabaseSQLTxCommitDoneInfo argument and performs certain actions based on the input.
func onTxCommit(
	databaseSQLTxEvents trace.Details,
	txCommit CounterVec,
	txCommitLatency TimerVec,
) func(info trace.DatabaseSQLTxCommitStartInfo) func(trace.DatabaseSQLTxCommitDoneInfo) {
	return func(info trace.DatabaseSQLTxCommitStartInfo) func(trace.DatabaseSQLTxCommitDoneInfo) {
		start := time.Now()

		return func(info trace.DatabaseSQLTxCommitDoneInfo) {
			if databaseSQLTxEvents != 0 {
				txCommit.With(map[string]string{
					"status": errorBrief(info.Error),
				}).Inc()
				txCommitLatency.With(nil).Record(time.Since(start))
			}
		}
	}
}

// onTxExec is a function that returns a callback function to be executed when a trace.DatabaseSQLTxExecDoneInfo event
// occurs.
func onTxExec(
	databaseSQLTxEvents trace.Details,
	txExec CounterVec,
	txExecLatency TimerVec,
) func(info trace.DatabaseSQLTxExecStartInfo) func(trace.DatabaseSQLTxExecDoneInfo) {
	return func(info trace.DatabaseSQLTxExecStartInfo) func(trace.DatabaseSQLTxExecDoneInfo) {
		start := time.Now()

		return func(info trace.DatabaseSQLTxExecDoneInfo) {
			if databaseSQLTxEvents != 0 {
				status := errorBrief(info.Error)
				txExec.With(map[string]string{
					"status": status,
				}).Inc()
				txExecLatency.With(nil).Record(time.Since(start))
			}
		}
	}
}

// onTxQuery is a callback function that measures trace events related to database transactions and queries
// in the "database/sql" package.
func onTxQuery(
	databaseSQLTxEvents trace.Details,
	txQuery CounterVec,
	txQueryLatency TimerVec,
) func(info trace.DatabaseSQLTxQueryStartInfo) func(trace.DatabaseSQLTxQueryDoneInfo) {
	return func(info trace.DatabaseSQLTxQueryStartInfo) func(trace.DatabaseSQLTxQueryDoneInfo) {
		start := time.Now()

		return func(info trace.DatabaseSQLTxQueryDoneInfo) {
			if databaseSQLTxEvents != 0 {
				status := errorBrief(info.Error)
				txQuery.With(map[string]string{
					"status": status,
				}).Inc()
				txQueryLatency.With(nil).Record(time.Since(start))
			}
		}
	}
}

// onTxRollback is a function that returns a closure. The closure takes in a `trace.DatabaseSQLTxRollbackStartInfo`
// argument and returns another closure that takes in a `trace.Database`.
func onTxRollback(
	databaseSQLTxEvents trace.Details,
	txRollback CounterVec,
	txRollbackLatency TimerVec,
) func(info trace.DatabaseSQLTxRollbackStartInfo) func(trace.DatabaseSQLTxRollbackDoneInfo) {
	return func(info trace.DatabaseSQLTxRollbackStartInfo) func(trace.DatabaseSQLTxRollbackDoneInfo) {
		start := time.Now()

		return func(info trace.DatabaseSQLTxRollbackDoneInfo) {
			if databaseSQLTxEvents != 0 {
				txRollback.With(map[string]string{
					"status": errorBrief(info.Error),
				}).Inc()
				txRollbackLatency.With(nil).Record(time.Since(start))
			}
		}
	}
}

// onConnExec measures the execution of a database/sql connection command.
func onConnExec(
	databaseSQLEvents trace.Details,
	databaseSQLConnEvents trace.Details,
	inflight GaugeVec,
	exec CounterVec,
	execLatency TimerVec,
) func(info trace.DatabaseSQLConnExecStartInfo) func(trace.DatabaseSQLConnExecDoneInfo) {
	return func(info trace.DatabaseSQLConnExecStartInfo) func(trace.DatabaseSQLConnExecDoneInfo) {
		if databaseSQLEvents != 0 {
			inflight.With(nil).Add(1)
		}
		var (
			mode  = info.Mode
			start = time.Now()
		)

		return func(info trace.DatabaseSQLConnExecDoneInfo) {
			if databaseSQLEvents != 0 {
				inflight.With(nil).Add(-1)
			}
			if databaseSQLConnEvents != 0 {
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

// onConnQuery handles the start and completion of a connection query event.
func onConnQuery(
	databaseSQLEvents trace.Details,
	databaseSQLConnEvents trace.Details,
	inflight GaugeVec,
	query CounterVec,
	queryLatency TimerVec,
) func(info trace.DatabaseSQLConnQueryStartInfo) func(trace.DatabaseSQLConnQueryDoneInfo) {
	return func(info trace.DatabaseSQLConnQueryStartInfo) func(trace.DatabaseSQLConnQueryDoneInfo) {
		if databaseSQLEvents != 0 {
			inflight.With(nil).Add(1)
		}
		var (
			mode  = info.Mode
			start = time.Now()
		)

		return func(info trace.DatabaseSQLConnQueryDoneInfo) {
			if databaseSQLEvents != 0 {
				inflight.With(nil).Add(-1)
			}
			if databaseSQLConnEvents != 0 {
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
