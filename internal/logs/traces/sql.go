package traces

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/logs"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// DatabaseSQL makes trace.DatabaseSQL with logging events from details
func DatabaseSQL(l logs.Logger, details trace.Details, opts ...Option) (t trace.DatabaseSQL) {
	if details&trace.DatabaseSQLEvents == 0 {
		return
	}
	options := ParseOptions(opts...)
	ll := newLogger(l, "database", "sql")
	if details&trace.DatabaseSQLConnectorEvents != 0 {
		//nolint:govet
		ll := ll.WithSubScope("connector")
		t.OnConnectorConnect = func(
			info trace.DatabaseSQLConnectorConnectStartInfo,
		) func(
			trace.DatabaseSQLConnectorConnectDoneInfo,
		) {
			ll.Trace("connect start")
			start := time.Now()
			return func(info trace.DatabaseSQLConnectorConnectDoneInfo) {
				if info.Error == nil {
					ll.Info(`connected`,
						logs.Duration("latency", time.Since(start)),
					)
				} else {
					ll.Error(`connect failed`,
						logs.Duration("latency", time.Since(start)),
						logs.Error(info.Error),
						logs.String("version", meta.Version),
					)
				}
			}
		}
	}
	//nolint:nestif
	if details&trace.DatabaseSQLConnEvents != 0 {
		//nolint:govet
		ll := ll.WithSubScope("conn")
		t.OnConnPing = func(info trace.DatabaseSQLConnPingStartInfo) func(trace.DatabaseSQLConnPingDoneInfo) {
			ll.Trace("ping start")
			start := time.Now()
			return func(info trace.DatabaseSQLConnPingDoneInfo) {
				if info.Error == nil {
					ll.Debug(`ping done`,
						logs.Duration("latency", time.Since(start)),
					)
				} else {
					ll.Error(`ping failed`,
						logs.Duration("latency", time.Since(start)),
						logs.Error(info.Error),
						logs.String("version", meta.Version),
					)
				}
			}
		}
		t.OnConnClose = func(info trace.DatabaseSQLConnCloseStartInfo) func(trace.DatabaseSQLConnCloseDoneInfo) {
			ll.Trace("close start")
			start := time.Now()
			return func(info trace.DatabaseSQLConnCloseDoneInfo) {
				if info.Error == nil {
					ll.Info(`closed`,
						logs.Duration("latency", time.Since(start)),
					)
				} else {
					ll.Error(`close failed`,
						logs.Duration("latency", time.Since(start)),
						logs.Error(info.Error),
						logs.String("version", meta.Version),
					)
				}
			}
		}
		t.OnConnBegin = func(info trace.DatabaseSQLConnBeginStartInfo) func(trace.DatabaseSQLConnBeginDoneInfo) {
			ll.Trace("begin transaction start")
			start := time.Now()
			return func(info trace.DatabaseSQLConnBeginDoneInfo) {
				if info.Error == nil {
					ll.Debug(`begin transaction was success`,
						logs.Duration("latency", time.Since(start)),
					)
				} else {
					ll.Error(`begin transaction failed`,
						logs.Duration("latency", time.Since(start)),
						logs.Error(info.Error),
						logs.String("version", meta.Version),
					)
				}
			}
		}
		t.OnConnPrepare = func(info trace.DatabaseSQLConnPrepareStartInfo) func(trace.DatabaseSQLConnPrepareDoneInfo) {
			if options.LogQuery {
				ll.Trace("prepare statement start",
					logs.String("query", info.Query),
				)
			} else {
				ll.Trace("prepare statement start")
			}
			query := info.Query
			start := time.Now()
			return func(info trace.DatabaseSQLConnPrepareDoneInfo) {
				if info.Error == nil {
					ll.Debug(`prepare statement was success`,
						logs.Duration("latency", time.Since(start)),
					)
				} else {
					if options.LogQuery {
						ll.Error(`prepare statement failed`,
							logs.Duration("latency", time.Since(start)),
							logs.String("query", query),
							logs.Error(info.Error),
							logs.String("version", meta.Version),
						)
					} else {
						ll.Error(`prepare statement failed`,
							logs.Duration("latency", time.Since(start)),
							logs.Error(info.Error),
							logs.String("version", meta.Version),
						)
					}
				}
			}
		}
		t.OnConnExec = func(info trace.DatabaseSQLConnExecStartInfo) func(trace.DatabaseSQLConnExecDoneInfo) {
			if options.LogQuery {
				ll.Trace("exec start",
					logs.String("query", info.Query),
				)
			} else {
				ll.Trace("exec start")
			}
			query := info.Query
			idempotent := info.Idempotent
			start := time.Now()
			return func(info trace.DatabaseSQLConnExecDoneInfo) {
				if info.Error == nil {
					ll.Debug(`exec was success`,
						logs.Duration("latency", time.Since(start)),
					)
				} else {
					m := retry.Check(info.Error)
					if options.LogQuery {
						ll.Error(`exec failed`,
							logs.Duration("latency", time.Since(start)),
							logs.String("query", query),
							logs.Error(info.Error),
							logs.Bool("retryable", m.MustRetry(idempotent)),
							logs.Int64("code", m.StatusCode()),
							logs.Bool("deleteSession", m.MustDeleteSession()),
							logs.String("version", meta.Version),
						)
					} else {
						ll.Error(`exec failed`,
							logs.Duration("latency", time.Since(start)),
							logs.Error(info.Error),
							logs.Bool("retryable", m.MustRetry(idempotent)),
							logs.Int64("code", m.StatusCode()),
							logs.Bool("deleteSession", m.MustDeleteSession()),
							logs.String("version", meta.Version),
						)
					}
				}
			}
		}
		t.OnConnQuery = func(info trace.DatabaseSQLConnQueryStartInfo) func(trace.DatabaseSQLConnQueryDoneInfo) {
			if options.LogQuery {
				ll.Trace("query start",
					logs.String("query", info.Query),
				)
			} else {
				ll.Trace("query start")
			}
			query := info.Query
			idempotent := info.Idempotent
			start := time.Now()
			return func(info trace.DatabaseSQLConnQueryDoneInfo) {
				if info.Error == nil {
					ll.Debug(`query was success`,
						logs.Duration("latency", time.Since(start)),
					)
				} else {
					m := retry.Check(info.Error)
					if options.LogQuery {
						ll.Error(`exec failed`,
							logs.Duration("latency", time.Since(start)),
							logs.String("query", query),
							logs.Error(info.Error),
							logs.Bool("retryable", m.MustRetry(idempotent)),
							logs.Int64("code", m.StatusCode()),
							logs.Bool("deleteSession", m.MustDeleteSession()),
							logs.String("version", meta.Version),
						)
					} else {
						ll.Error(`exec failed`,
							logs.Duration("latency", time.Since(start)),
							logs.Error(info.Error),
							logs.Bool("retryable", m.MustRetry(idempotent)),
							logs.Int64("code", m.StatusCode()),
							logs.Bool("deleteSession", m.MustDeleteSession()),
							logs.String("version", meta.Version),
						)
					}
				}
			}
		}
	}
	if details&trace.DatabaseSQLTxEvents != 0 {
		//nolint:govet
		ll := ll.WithSubScope("tx")
		t.OnTxCommit = func(info trace.DatabaseSQLTxCommitStartInfo) func(trace.DatabaseSQLTxCommitDoneInfo) {
			ll.Trace("commit start")
			start := time.Now()
			return func(info trace.DatabaseSQLTxCommitDoneInfo) {
				if info.Error == nil {
					ll.Debug(`committed`,
						logs.Duration("latency", time.Since(start)),
					)
				} else {
					ll.Error(`commit failed`,
						logs.Duration("latency", time.Since(start)),
						logs.Error(info.Error),
						logs.String("version", meta.Version),
					)
				}
			}
		}
		t.OnTxRollback = func(info trace.DatabaseSQLTxRollbackStartInfo) func(trace.DatabaseSQLTxRollbackDoneInfo) {
			ll.Trace("rollback start")
			start := time.Now()
			return func(info trace.DatabaseSQLTxRollbackDoneInfo) {
				if info.Error == nil {
					ll.Debug(`rollbacked`,
						logs.Duration("latency", time.Since(start)),
					)
				} else {
					ll.Error(`rollback failed`,
						logs.Duration("latency", time.Since(start)),
						logs.Error(info.Error),
						logs.String("version", meta.Version),
					)
				}
			}
		}
	}
	//nolint:nestif
	if details&trace.DatabaseSQLStmtEvents != 0 {
		//nolint:govet
		ll := ll.WithSubScope("stmt")
		t.OnStmtClose = func(info trace.DatabaseSQLStmtCloseStartInfo) func(trace.DatabaseSQLStmtCloseDoneInfo) {
			ll.Trace("close start")
			start := time.Now()
			return func(info trace.DatabaseSQLStmtCloseDoneInfo) {
				if info.Error == nil {
					ll.Trace(`closed`,
						logs.Duration("latency", time.Since(start)),
					)
				} else {
					ll.Error(`close failed`,
						logs.Duration("latency", time.Since(start)),
						logs.Error(info.Error),
						logs.String("version", meta.Version),
					)
				}
			}
		}
		t.OnStmtExec = func(info trace.DatabaseSQLStmtExecStartInfo) func(trace.DatabaseSQLStmtExecDoneInfo) {
			if options.LogQuery {
				ll.Trace("exec start",
					logs.String("query", info.Query),
				)
			} else {
				ll.Trace("exec start")
			}
			query := info.Query
			start := time.Now()
			return func(info trace.DatabaseSQLStmtExecDoneInfo) {
				if info.Error == nil {
					ll.Debug(`exec was success`,
						logs.Duration("latency", time.Since(start)),
						logs.Error(info.Error),
						logs.String("version", meta.Version),
					)
				} else {
					if options.LogQuery {
						ll.Error(`exec failed`,
							logs.Duration("latency", time.Since(start)),
							logs.String("query", query),
							logs.Error(info.Error),
							logs.String("version", meta.Version),
						)
					} else {
						ll.Error(`exec failed`,
							logs.Duration("latency", time.Since(start)),
							logs.Error(info.Error),
							logs.String("version", meta.Version),
						)
					}
				}
			}
		}
		t.OnStmtQuery = func(info trace.DatabaseSQLStmtQueryStartInfo) func(trace.DatabaseSQLStmtQueryDoneInfo) {
			if options.LogQuery {
				ll.Trace("query start",
					logs.String("query", info.Query),
				)
			} else {
				ll.Trace("query start")
			}
			query := info.Query
			start := time.Now()
			return func(info trace.DatabaseSQLStmtQueryDoneInfo) {
				if info.Error == nil {
					ll.Debug(`query was success`,
						logs.Duration("latency", time.Since(start)),
					)
				} else {
					if options.LogQuery {
						ll.Error(`query failed`,
							logs.Duration("latency", time.Since(start)),
							logs.String("query", query),
							logs.Error(info.Error),
							logs.String("version", meta.Version),
						)
					} else {
						ll.Error(`query failed`,
							logs.Duration("latency", time.Since(start)),
							logs.Error(info.Error),
							logs.String("version", meta.Version),
						)
					}
				}
			}
		}
	}
	return t
}
