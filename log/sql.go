package log

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/meta"
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
			l.Tracef("connect start")
			start := time.Now()
			return func(info trace.DatabaseSQLConnectorConnectDoneInfo) {
				if info.Error == nil {
					l.Infof(`connected {latency:"%v"}`,
						time.Since(start),
					)
				} else {
					l.Errorf(`connect failed {latency:"%v",error:"%v",version:"%s"}`,
						time.Since(start),
						info.Error,
						meta.Version,
					)
				}
			}
		}
	}
	//nolint:nestif
	if details&trace.DatabaseSQLConnEvents != 0 {
		//nolint:govet
		l := l.WithName(`conn`)
		t.OnConnPing = func(info trace.DatabaseSQLConnPingStartInfo) func(trace.DatabaseSQLConnPingDoneInfo) {
			l.Tracef("ping start")
			start := time.Now()
			return func(info trace.DatabaseSQLConnPingDoneInfo) {
				if info.Error == nil {
					l.Debugf(`ping done {latency:"%v"}`,
						time.Since(start),
					)
				} else {
					l.Errorf(`ping failed {latency:"%v",error:"%v",version:"%s"}`,
						time.Since(start),
						info.Error,
						meta.Version,
					)
				}
			}
		}
		t.OnConnClose = func(info trace.DatabaseSQLConnCloseStartInfo) func(trace.DatabaseSQLConnCloseDoneInfo) {
			l.Tracef("close start")
			start := time.Now()
			return func(info trace.DatabaseSQLConnCloseDoneInfo) {
				if info.Error == nil {
					l.Infof(`closed {latency:"%v"}`,
						time.Since(start),
					)
				} else {
					l.Errorf(`close failed {latency:"%v",error:"%v",version:"%s"}`,
						time.Since(start),
						info.Error,
						meta.Version,
					)
				}
			}
		}
		t.OnConnBegin = func(info trace.DatabaseSQLConnBeginStartInfo) func(trace.DatabaseSQLConnBeginDoneInfo) {
			l.Tracef("begin transaction start")
			start := time.Now()
			return func(info trace.DatabaseSQLConnBeginDoneInfo) {
				if info.Error == nil {
					l.Debugf(`begin transaction was success {latency:"%v"}`,
						time.Since(start),
					)
				} else {
					l.Errorf(`begin transaction failed {latency:"%v",error:"%v",version:"%s"}`,
						time.Since(start),
						info.Error,
						meta.Version,
					)
				}
			}
		}
		t.OnConnPrepare = func(info trace.DatabaseSQLConnPrepareStartInfo) func(trace.DatabaseSQLConnPrepareDoneInfo) {
			if options.logQuery {
				l.Tracef("prepare statement start {query:\"%s\"}",
					info.Query,
				)
			} else {
				l.Tracef("prepare statement start")
			}
			query := info.Query
			start := time.Now()
			return func(info trace.DatabaseSQLConnPrepareDoneInfo) {
				if info.Error == nil {
					l.Debugf(`prepare statement was success {latency:"%v"}`,
						time.Since(start),
					)
				} else {
					if options.logQuery {
						l.Errorf(`prepare statement failed {latency:"%v",query:"%s",error:"%v",version:"%s"}`,
							time.Since(start),
							query,
							info.Error,
							meta.Version,
						)
					} else {
						l.Errorf(`prepare statement failed {latency:"%v",error:"%v",version:"%s"}`,
							time.Since(start),
							info.Error,
							meta.Version,
						)
					}
				}
			}
		}
		t.OnConnExec = func(info trace.DatabaseSQLConnExecStartInfo) func(trace.DatabaseSQLConnExecDoneInfo) {
			if options.logQuery {
				l.Tracef("exec start {query:\"%s\"}",
					info.Query,
				)
			} else {
				l.Tracef("exec start")
			}
			query := info.Query
			idempotent := info.Idempotent
			start := time.Now()
			return func(info trace.DatabaseSQLConnExecDoneInfo) {
				if info.Error == nil {
					l.Debugf(`exec was success {latency:"%v"}`,
						time.Since(start),
					)
				} else {
					m := retry.Check(info.Error)
					if options.logQuery {
						l.Errorf(`exec failed {latency:"%v",query:"%s",error:"%v",retryable:%v,code:%d,deleteSession:%v,version:"%s"}`,
							time.Since(start),
							query,
							info.Error,
							m.MustRetry(idempotent),
							m.StatusCode(),
							m.MustDeleteSession(),
							meta.Version,
						)
					} else {
						l.Errorf(`exec failed {latency:"%v",error:"%v",retryable:%v,code:%d,deleteSession:%v,version:"%s"}`,
							time.Since(start),
							info.Error,
							m.MustRetry(idempotent),
							m.StatusCode(),
							m.MustDeleteSession(),
							meta.Version,
						)
					}
				}
			}
		}
		t.OnConnQuery = func(info trace.DatabaseSQLConnQueryStartInfo) func(trace.DatabaseSQLConnQueryDoneInfo) {
			if options.logQuery {
				l.Tracef("query start {query:\"%s\"}",
					info.Query,
				)
			} else {
				l.Tracef("query start")
			}
			query := info.Query
			idempotent := info.Idempotent
			start := time.Now()
			return func(info trace.DatabaseSQLConnQueryDoneInfo) {
				if info.Error == nil {
					l.Debugf(`query was success {latency:"%v"}`,
						time.Since(start),
					)
				} else {
					m := retry.Check(info.Error)
					if options.logQuery {
						l.Errorf(`exec failed {latency:"%v",query:"%s",error:"%v",retryable:%v,code:%d,deleteSession:%v,version:"%s"}`,
							time.Since(start),
							query,
							info.Error,
							m.MustRetry(idempotent),
							m.StatusCode(),
							m.MustDeleteSession(),
							meta.Version,
						)
					} else {
						l.Errorf(`exec failed {latency:"%v",error:"%v",retryable:%v,code:%d,deleteSession:%v,version:"%s"}`,
							time.Since(start),
							info.Error,
							m.MustRetry(idempotent),
							m.StatusCode(),
							m.MustDeleteSession(),
							meta.Version,
						)
					}
				}
			}
		}
	}
	if details&trace.DatabaseSQLTxEvents != 0 {
		//nolint:govet
		l := l.WithName(`tx`)
		t.OnTxCommit = func(info trace.DatabaseSQLTxCommitStartInfo) func(trace.DatabaseSQLTxCommitDoneInfo) {
			l.Tracef("commit start")
			start := time.Now()
			return func(info trace.DatabaseSQLTxCommitDoneInfo) {
				if info.Error == nil {
					l.Debugf(`committed {latency:"%v"}`,
						time.Since(start),
					)
				} else {
					l.Errorf(`commit failed {latency:"%v",error:"%v",version:"%s"}`,
						time.Since(start),
						info.Error,
						meta.Version,
					)
				}
			}
		}
		t.OnTxRollback = func(info trace.DatabaseSQLTxRollbackStartInfo) func(trace.DatabaseSQLTxRollbackDoneInfo) {
			l.Tracef("rollback start")
			start := time.Now()
			return func(info trace.DatabaseSQLTxRollbackDoneInfo) {
				if info.Error == nil {
					l.Debugf(`rollbacked {latency:"%v"}`,
						time.Since(start),
					)
				} else {
					l.Errorf(`rollback failed {latency:"%v",error:"%v",version:"%s"}`,
						time.Since(start),
						info.Error,
						meta.Version,
					)
				}
			}
		}
	}
	//nolint:nestif
	if details&trace.DatabaseSQLStmtEvents != 0 {
		//nolint:govet
		l := l.WithName(`stmt`)
		t.OnStmtClose = func(info trace.DatabaseSQLStmtCloseStartInfo) func(trace.DatabaseSQLStmtCloseDoneInfo) {
			l.Tracef("close start")
			start := time.Now()
			return func(info trace.DatabaseSQLStmtCloseDoneInfo) {
				if info.Error == nil {
					l.Tracef(`closed {latency:"%v"}`,
						time.Since(start),
					)
				} else {
					l.Errorf(`close failed {latency:"%v",error:"%v",version:"%s"}`,
						time.Since(start),
						info.Error,
						meta.Version,
					)
				}
			}
		}
		t.OnStmtExec = func(info trace.DatabaseSQLStmtExecStartInfo) func(trace.DatabaseSQLStmtExecDoneInfo) {
			if options.logQuery {
				l.Tracef("exec start {query:\"%s\"}",
					info.Query,
				)
			} else {
				l.Tracef("exec start")
			}
			query := info.Query
			start := time.Now()
			return func(info trace.DatabaseSQLStmtExecDoneInfo) {
				if info.Error == nil {
					l.Debugf(`exec was success {latency:"%v"}`,
						time.Since(start),
					)
				} else {
					if options.logQuery {
						l.Errorf(`exec failed {latency:"%v",query:"%s",error:"%v",version:"%s"}`,
							time.Since(start),
							query,
							info.Error,
							meta.Version,
						)
					} else {
						l.Errorf(`exec failed {latency:"%v",error:"%v",version:"%s"}`,
							time.Since(start),
							info.Error,
							meta.Version,
						)
					}
				}
			}
		}
		t.OnStmtQuery = func(info trace.DatabaseSQLStmtQueryStartInfo) func(trace.DatabaseSQLStmtQueryDoneInfo) {
			if options.logQuery {
				l.Tracef("query start {query:\"%s\"}",
					info.Query,
				)
			} else {
				l.Tracef("query start")
			}
			query := info.Query
			start := time.Now()
			return func(info trace.DatabaseSQLStmtQueryDoneInfo) {
				if info.Error == nil {
					l.Debugf(`query was success {latency:"%v"}`,
						time.Since(start),
					)
				} else {
					if options.logQuery {
						l.Errorf(`query failed {latency:"%v",query:"%s",error:"%v",version:"%s"}`,
							time.Since(start),
							query,
							info.Error,
							meta.Version,
						)
					} else {
						l.Errorf(`query failed {latency:"%v",error:"%v",version:"%s"}`,
							time.Since(start),
							info.Error,
							meta.Version,
						)
					}
				}
			}
		}
	}
	return t
}
