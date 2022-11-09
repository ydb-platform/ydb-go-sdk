package structural

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// DatabaseSQL makes trace.DatabaseSQL with logging events from details
func DatabaseSQL(l Logger, details trace.Details, opts ...option) (t trace.DatabaseSQL) {
	if details&trace.DatabaseSQLEvents == 0 {
		return
	}
	options := parseOptions(opts...)
	l = l.WithName(`database`).WithName(`sql`)
	if details&trace.DatabaseSQLConnectorEvents != 0 {
		//nolint:govet
		l := l.WithName(`connector`)
		t.OnConnectorConnect = func(
			info trace.DatabaseSQLConnectorConnectStartInfo,
		) func(
			trace.DatabaseSQLConnectorConnectDoneInfo,
		) {
			l.Trace().Message("connect start")
			start := time.Now()
			return func(info trace.DatabaseSQLConnectorConnectDoneInfo) {
				if info.Error == nil {
					l.Info().
						Duration("latency", time.Since(start)).
						Message("connected")
				} else {
					l.Error().
						Duration("latency", time.Since(start)).
						Error(info.Error).
						String("version", meta.Version).
						Message("connect failed")
				}
			}
		}
	}
	//nolint:nestif
	if details&trace.DatabaseSQLConnEvents != 0 {
		//nolint:govet
		l := l.WithName(`conn`)
		t.OnConnPing = func(info trace.DatabaseSQLConnPingStartInfo) func(trace.DatabaseSQLConnPingDoneInfo) {
			l.Trace().Message("ping start")
			start := time.Now()
			return func(info trace.DatabaseSQLConnPingDoneInfo) {
				if info.Error == nil {
					l.Debug().
						Duration("latency", time.Since(start)).
						Message("ping done")
				} else {
					l.Error().
						Duration("latency", time.Since(start)).
						Error(info.Error).
						String("version", meta.Version).
						Message("ping failed")
				}
			}
		}
		t.OnConnClose = func(info trace.DatabaseSQLConnCloseStartInfo) func(trace.DatabaseSQLConnCloseDoneInfo) {
			l.Trace().Message("close start")
			start := time.Now()
			return func(info trace.DatabaseSQLConnCloseDoneInfo) {
				if info.Error == nil {
					l.Info().
						Duration("latency", time.Since(start)).
						Message("closed")
				} else {
					l.Error().
						Duration("latency", time.Since(start)).
						Error(info.Error).
						String("version", meta.Version).
						Message("close failed")
				}
			}
		}
		t.OnConnBegin = func(info trace.DatabaseSQLConnBeginStartInfo) func(trace.DatabaseSQLConnBeginDoneInfo) {
			l.Trace().Message("begin transaction start")
			start := time.Now()
			return func(info trace.DatabaseSQLConnBeginDoneInfo) {
				if info.Error == nil {
					l.Debug().
						Duration("latency", time.Since(start)).
						Message("begin transaction was success")
				} else {
					l.Error().
						Duration("latency", time.Since(start)).
						Error(info.Error).
						String("version", meta.Version).
						Message("begin transaction failed")
				}
			}
		}
		t.OnConnPrepare = func(info trace.DatabaseSQLConnPrepareStartInfo) func(trace.DatabaseSQLConnPrepareDoneInfo) {
			if options.logQuery {
				l.Trace().
					String("query", info.Query).
					Message("prepare statement start")
			} else {
				l.Trace().Message("prepare statement start")
			}
			query := info.Query
			start := time.Now()
			return func(info trace.DatabaseSQLConnPrepareDoneInfo) {
				if info.Error == nil {
					l.Debug().
						Duration("latency", time.Since(start)).
						Message("prepare statement was success")
				} else {
					if options.logQuery {
						l.Error().
							Duration("latency", time.Since(start)).
							String("query", query).
							Error(info.Error).
							String("version", meta.Version).
							Message("prepare statement failed")
					} else {
						l.Error().
							Duration("latency", time.Since(start)).
							Error(info.Error).
							String("version", meta.Version).
							Message("prepare statement failed")
					}
				}
			}
		}
		t.OnConnExec = func(info trace.DatabaseSQLConnExecStartInfo) func(trace.DatabaseSQLConnExecDoneInfo) {
			if options.logQuery {
				l.Trace().
					String("query", info.Query).
					Message("exec start")
			} else {
				l.Trace().Message("exec start")
			}
			query := info.Query
			idempotent := info.Idempotent
			start := time.Now()
			return func(info trace.DatabaseSQLConnExecDoneInfo) {
				if info.Error == nil {
					l.Debug().
						Duration("latency", time.Since(start)).
						Message("exec was success")
				} else {
					m := retry.Check(info.Error)
					if options.logQuery {
						l.Error().
							Duration("latency", time.Since(start)).
							String("query", query).
							Error(info.Error).
							Bool("retryable", m.MustRetry(idempotent)).
							Int64("code", m.StatusCode()).
							Bool("deleteSession", m.MustDeleteSession()).
							String("version", meta.Version).
							Message("exec failed")
					} else {
						l.Error().
							Duration("latency", time.Since(start)).
							Error(info.Error).
							Bool("retryable", m.MustRetry(idempotent)).
							Int64("code", m.StatusCode()).
							Bool("deleteSession", m.MustDeleteSession()).
							String("version", meta.Version).
							Message("exec failed")
					}
				}
			}
		}
		t.OnConnQuery = func(info trace.DatabaseSQLConnQueryStartInfo) func(trace.DatabaseSQLConnQueryDoneInfo) {
			if options.logQuery {
				l.Trace().
					String("query", info.Query).
					Message("query start")
			} else {
				l.Trace().Message("query start")
			}
			query := info.Query
			idempotent := info.Idempotent
			start := time.Now()
			return func(info trace.DatabaseSQLConnQueryDoneInfo) {
				if info.Error == nil {
					l.Debug().
						Duration("latency", time.Since(start)).
						Message("query was success")
				} else {
					m := retry.Check(info.Error)
					if options.logQuery {
						l.Error().
							Duration("latency", time.Since(start)).
							String("query", query).
							Error(info.Error).
							Bool("retryable", m.MustRetry(idempotent)).
							Int64("code", m.StatusCode()).
							Bool("deleteSession", m.MustDeleteSession()).
							String("version", meta.Version).
							Message("query failed")
					} else {
						l.Error().
							Duration("latency", time.Since(start)).
							Error(info.Error).
							Bool("retryable", m.MustRetry(idempotent)).
							Int64("code", m.StatusCode()).
							Bool("deleteSession", m.MustDeleteSession()).
							String("version", meta.Version).
							Message("query failed")
					}
				}
			}
		}
	}
	if details&trace.DatabaseSQLTxEvents != 0 {
		//nolint:govet
		l := l.WithName(`tx`)
		t.OnTxCommit = func(info trace.DatabaseSQLTxCommitStartInfo) func(trace.DatabaseSQLTxCommitDoneInfo) {
			l.Trace().Message("commit start")
			start := time.Now()
			return func(info trace.DatabaseSQLTxCommitDoneInfo) {
				if info.Error == nil {
					l.Debug().
						Duration("latency", time.Since(start)).
						Message("committed")
				} else {
					l.Error().
						Duration("latency", time.Since(start)).
						Error(info.Error).
						String("version", meta.Version).
						Message("commit failed")
				}
			}
		}
		t.OnTxRollback = func(info trace.DatabaseSQLTxRollbackStartInfo) func(trace.DatabaseSQLTxRollbackDoneInfo) {
			l.Trace().Message("rollback start")
			start := time.Now()
			return func(info trace.DatabaseSQLTxRollbackDoneInfo) {
				if info.Error == nil {
					l.Debug().
						Duration("latency", time.Since(start)).
						Message("rollbacked")
				} else {
					l.Error().
						Duration("latency", time.Since(start)).
						Error(info.Error).
						String("version", meta.Version).
						Message("rollback failed")
				}
			}
		}
	}
	//nolint:nestif
	if details&trace.DatabaseSQLStmtEvents != 0 {
		//nolint:govet
		l := l.WithName(`stmt`)
		t.OnStmtClose = func(info trace.DatabaseSQLStmtCloseStartInfo) func(trace.DatabaseSQLStmtCloseDoneInfo) {
			l.Trace().Message("close start")
			start := time.Now()
			return func(info trace.DatabaseSQLStmtCloseDoneInfo) {
				if info.Error == nil {
					l.Trace().
						Duration("latency", time.Since(start)).
						Message("closed")
				} else {
					l.Error().
						Duration("latency", time.Since(start)).
						Error(info.Error).
						String("version", meta.Version).
						Message("close failed")
				}
			}
		}
		t.OnStmtExec = func(info trace.DatabaseSQLStmtExecStartInfo) func(trace.DatabaseSQLStmtExecDoneInfo) {
			if options.logQuery {
				l.Trace().
					String("query", info.Query).
					Message("exec start")
			} else {
				l.Trace().Message("exec start")
			}
			query := info.Query
			start := time.Now()
			return func(info trace.DatabaseSQLStmtExecDoneInfo) {
				if info.Error == nil {
					l.Debug().
						Duration("latency", time.Since(start)).
						Message("exec was success")
				} else {
					if options.logQuery {
						l.Error().
							Duration("latency", time.Since(start)).
							String("query", query).
							Error(info.Error).
							String("version", meta.Version).
							Message("exec failed")
					} else {
						l.Error().
							Duration("latency", time.Since(start)).
							Error(info.Error).
							String("version", meta.Version).
							Message("exec failed")
					}
				}
			}
		}
		t.OnStmtQuery = func(info trace.DatabaseSQLStmtQueryStartInfo) func(trace.DatabaseSQLStmtQueryDoneInfo) {
			if options.logQuery {
				l.Trace().
					String("query", info.Query).
					Message("query start")
			} else {
				l.Trace().Message("query start")
			}
			query := info.Query
			start := time.Now()
			return func(info trace.DatabaseSQLStmtQueryDoneInfo) {
				if info.Error == nil {
					l.Debug().
						Duration("latency", time.Since(start)).
						Message("query was success")
				} else {
					if options.logQuery {
						l.Error().
							Duration("latency", time.Since(start)).
							String("query", query).
							Error(info.Error).
							String("version", meta.Version).
							Message("query failed")
					} else {
						l.Error().
							Duration("latency", time.Since(start)).
							Error(info.Error).
							String("version", meta.Version).
							Message("query failed")
					}
				}
			}
		}
	}
	return t
}
