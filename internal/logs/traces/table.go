package traces

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/logs"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Table makes trace.Table with logging events from details
//
//nolint:gocyclo
func Table(l logs.Logger, details trace.Details, opts ...Option) (t trace.Table) {
	if details&trace.TableEvents == 0 {
		return
	}
	options := ParseOptions(opts...)
	ll := newLogger(l, "table")
	t.OnDo = func(
		info trace.TableDoStartInfo,
	) func(
		info trace.TableDoIntermediateInfo,
	) func(
		trace.TableDoDoneInfo,
	) {
		idempotent := info.Idempotent
		ll.Trace(`do start`,
			logs.Bool("idempotent", idempotent),
		)
		start := time.Now()
		return func(info trace.TableDoIntermediateInfo) func(trace.TableDoDoneInfo) {
			if info.Error == nil {
				ll.Trace(`do attempt done`,
					logs.Duration("latency", time.Since(start)),
					logs.Bool("idempotent", idempotent),
				)
			} else {
				f := ll.Warn
				if !xerrors.IsYdb(info.Error) {
					f = ll.Debug
				}
				m := retry.Check(info.Error)
				f(`do attempt failed`,
					logs.Duration("latency", time.Since(start)),
					logs.Bool("idempotent", idempotent),
					logs.Error(info.Error),
					logs.Bool("retryable", m.MustRetry(idempotent)),
					logs.Int64("code", m.StatusCode()),
					logs.Bool("deleteSession", m.MustDeleteSession()),
					logs.String("version", meta.Version),
				)
			}
			return func(info trace.TableDoDoneInfo) {
				if info.Error == nil {
					ll.Trace(`do done`,
						logs.Duration("latency", time.Since(start)),
						logs.Bool("idempotent", idempotent),
						logs.Int("attempts", info.Attempts),
					)
				} else {
					f := ll.Error
					if !xerrors.IsYdb(info.Error) {
						f = ll.Debug
					}
					m := retry.Check(info.Error)
					f(`do failed {latency:"%v",idempotent:%t,attempts:%d,error:"%s",retryable:%t,code:%d,deleteSession:%t}`,
						logs.Duration("latency", time.Since(start)),
						logs.Bool("idempotent", idempotent),
						logs.Int("attempts", info.Attempts),
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
	t.OnDoTx = func(
		info trace.TableDoTxStartInfo,
	) func(
		info trace.TableDoTxIntermediateInfo,
	) func(
		trace.TableDoTxDoneInfo,
	) {
		idempotent := info.Idempotent
		ll.Trace(`doTx start`,
			logs.Bool("idempotent", idempotent),
		)
		start := time.Now()
		return func(info trace.TableDoTxIntermediateInfo) func(trace.TableDoTxDoneInfo) {
			if info.Error == nil {
				ll.Trace(`doTx attempt done`,
					logs.Duration("latency", time.Since(start)),
					logs.Bool("idempotent", idempotent),
				)
			} else {
				f := ll.Warn
				if !xerrors.IsYdb(info.Error) {
					f = ll.Debug
				}
				m := retry.Check(info.Error)
				f(`doTx attempt failed`,
					logs.Duration("latency", time.Since(start)),
					logs.Bool("idempotent", idempotent),
					logs.Error(info.Error),
					logs.Bool("retryable", m.MustRetry(idempotent)),
					logs.Int64("code", m.StatusCode()),
					logs.Bool("deleteSession", m.MustDeleteSession()),
					logs.String("version", meta.Version),
				)
			}
			return func(info trace.TableDoTxDoneInfo) {
				if info.Error == nil {
					ll.Trace(`doTx done`,
						logs.Duration("latency", time.Since(start)),
						logs.Bool("idempotent", idempotent),
						logs.Int("attempts", info.Attempts),
					)
				} else {
					f := ll.Warn
					if !xerrors.IsYdb(info.Error) {
						f = ll.Debug
					}
					m := retry.Check(info.Error)
					f(`doTx failed`,
						logs.Duration("latency", time.Since(start)),
						logs.Bool("idempotent", idempotent),
						logs.Int("attempts", info.Attempts),
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
	t.OnCreateSession = func(
		info trace.TableCreateSessionStartInfo,
	) func(
		info trace.TableCreateSessionIntermediateInfo,
	) func(
		trace.TableCreateSessionDoneInfo,
	) {
		ll.Trace(`create session start`)
		start := time.Now()
		return func(info trace.TableCreateSessionIntermediateInfo) func(trace.TableCreateSessionDoneInfo) {
			if info.Error == nil {
				ll.Trace(`create session intermediate`,
					logs.Duration("latency", time.Since(start)),
				)
			} else {
				ll.Error(`create session intermediate`,
					logs.Duration("latency", time.Since(start)),
					logs.Error(info.Error),
					logs.String("version", meta.Version),
				)
			}
			return func(info trace.TableCreateSessionDoneInfo) {
				if info.Error == nil {
					ll.Trace(`create session done`,
						logs.Duration("latency", time.Since(start)),
						logs.Int("attempts", info.Attempts),
						logs.String("session_id", info.Session.ID()),
						logs.String("session_status", info.Session.Status()),
					)
				} else {
					ll.Error(`create session failed`,
						logs.Duration("latency", time.Since(start)),
						logs.Int("attempts", info.Attempts),
						logs.Error(info.Error),
						logs.String("version", meta.Version),
					)
				}
			}
		}
	}
	//nolint:nestif
	if details&trace.TableSessionEvents != 0 {
		//nolint:govet
		ll := ll.WithSubScope("session")
		if details&trace.TableSessionLifeCycleEvents != 0 {
			t.OnSessionNew = func(info trace.TableSessionNewStartInfo) func(trace.TableSessionNewDoneInfo) {
				ll.Trace(`create start`)
				start := time.Now()
				return func(info trace.TableSessionNewDoneInfo) {
					if info.Error == nil {
						if info.Session != nil {
							ll.Trace(`create done`,
								logs.Duration("latency", time.Since(start)),
								logs.String("id", info.Session.ID()),
							)
						} else {
							ll.Warn(`create done without session`,
								logs.Duration("latency", time.Since(start)),
								logs.String("version", meta.Version),
							)
						}
					} else {
						ll.Warn(`create failed`,
							logs.Duration("latency", time.Since(start)),
							logs.Error(info.Error),
							logs.String("version", meta.Version),
						)
					}
				}
			}
			t.OnSessionDelete = func(info trace.TableSessionDeleteStartInfo) func(trace.TableSessionDeleteDoneInfo) {
				session := info.Session
				ll.Trace(`delete start`,
					logs.String("id", info.Session.ID()),
					logs.String("status", info.Session.Status()),
				)
				start := time.Now()
				return func(info trace.TableSessionDeleteDoneInfo) {
					if info.Error == nil {
						ll.Trace(`delete done`,
							logs.Duration("latency", time.Since(start)),
							logs.String("id", session.ID()),
							logs.String("status", session.Status()),
						)
					} else {
						ll.Warn(`delete failed`,
							logs.Duration("latency", time.Since(start)),
							logs.String("id", session.ID()),
							logs.String("status", session.Status()),
							logs.Error(info.Error),
							logs.String("version", meta.Version),
						)
					}
				}
			}
			t.OnSessionKeepAlive = func(info trace.TableKeepAliveStartInfo) func(trace.TableKeepAliveDoneInfo) {
				session := info.Session
				ll.Trace(`keep-alive start`,
					logs.String("id", session.ID()),
					logs.String("status", session.Status()),
				)
				start := time.Now()
				return func(info trace.TableKeepAliveDoneInfo) {
					if info.Error == nil {
						ll.Trace(`keep-alive done`,
							logs.Duration("latency", time.Since(start)),
							logs.String("id", session.ID()),
							logs.String("status", session.Status()),
						)
					} else {
						ll.Warn(`keep-alive failed`,
							logs.Duration("latency", time.Since(start)),
							logs.String("id", session.ID()),
							logs.String("status", session.Status()),
							logs.Error(info.Error),
							logs.String("version", meta.Version),
						)
					}
				}
			}
		}
		if details&trace.TableSessionQueryEvents != 0 {
			//nolint:govet
			ll := ll.WithSubScope("query")
			if details&trace.TableSessionQueryInvokeEvents != 0 {
				//nolint:govet
				ll := ll.WithSubScope("invoke")
				t.OnSessionQueryPrepare = func(
					info trace.TablePrepareDataQueryStartInfo,
				) func(
					trace.TablePrepareDataQueryDoneInfo,
				) {
					session := info.Session
					query := info.Query
					if options.LogQuery {
						ll.Trace(`prepare start`,
							logs.String("id", session.ID()),
							logs.String("status", session.Status()),
							logs.String("query", query),
						)
					} else {
						ll.Trace(`prepare start`,
							logs.String("id", session.ID()),
							logs.String("status", session.Status()),
						)
					}
					start := time.Now()
					return func(info trace.TablePrepareDataQueryDoneInfo) {
						if info.Error == nil {
							if options.LogQuery {
								ll.Debug(`prepare done`,
									logs.Duration("latency", time.Since(start)),
									logs.String("id", session.ID()),
									logs.String("status", session.Status()),
									logs.String("query", query),
									logs.Stringer("result", info.Result),
								)
							} else {
								ll.Debug(`prepare done`,
									logs.Duration("latency", time.Since(start)),
									logs.String("id", session.ID()),
									logs.String("status", session.Status()),
									logs.Stringer("result", info.Result),
								)
							}
						} else {
							if options.LogQuery {
								ll.Error(`prepare failed`,
									logs.Duration("latency", time.Since(start)),
									logs.String("id", session.ID()),
									logs.String("status", session.Status()),
									logs.String("query", query),
									logs.Error(info.Error),
									logs.String("version", meta.Version),
								)
							} else {
								ll.Error(`prepare failed`,
									logs.Duration("latency", time.Since(start)),
									logs.String("id", session.ID()),
									logs.String("status", session.Status()),
									logs.Error(info.Error),
									logs.String("version", meta.Version),
								)
							}
						}
					}
				}
				t.OnSessionQueryExecute = func(
					info trace.TableExecuteDataQueryStartInfo,
				) func(
					trace.TableExecuteDataQueryDoneInfo,
				) {
					session := info.Session
					query := info.Query
					params := info.Parameters
					if options.LogQuery {
						ll.Trace(`execute start`,
							logs.String("id", session.ID()),
							logs.String("status", session.Status()),
							logs.Stringer("query", query),
							logs.Stringer("params", params),
						)
					} else {
						ll.Trace(`execute start`,
							logs.String("id", session.ID()),
							logs.String("status", session.Status()),
						)
					}
					start := time.Now()
					return func(info trace.TableExecuteDataQueryDoneInfo) {
						if info.Error == nil {
							tx := info.Tx
							if options.LogQuery {
								ll.Debug(`execute done`,
									logs.Duration("latency", time.Since(start)),
									logs.String("id", session.ID()),
									logs.String("status", session.Status()),
									logs.String("tx", tx.ID()),
									logs.Stringer("query", query),
									logs.Stringer("params", params),
									logs.Bool("prepared", info.Prepared),
									logs.NamedError("result_err", info.Result.Err()),
								)
							} else {
								ll.Debug(`execute done`,
									logs.Duration("latency", time.Since(start)),
									logs.String("id", session.ID()),
									logs.String("status", session.Status()),
									logs.String("tx", tx.ID()),
									logs.Bool("prepared", info.Prepared),
									logs.NamedError("result_err", info.Result.Err()),
								)
							}
						} else {
							if options.LogQuery {
								ll.Error(`execute failed`,
									logs.Duration("latency", time.Since(start)),
									logs.String("id", session.ID()),
									logs.String("status", session.Status()),
									logs.Stringer("query", query),
									logs.Stringer("params", params),
									logs.Bool("prepared", info.Prepared),
									logs.Error(info.Error),
									logs.String("version", meta.Version),
								)
							} else {
								ll.Error(`execute failed`,
									logs.Duration("latency", time.Since(start)),
									logs.String("id", session.ID()),
									logs.String("status", session.Status()),
									logs.Bool("prepared", info.Prepared),
									logs.Error(info.Error),
									logs.String("version", meta.Version),
								)
							}
						}
					}
				}
			}
			if details&trace.TableSessionQueryStreamEvents != 0 {
				//nolint:govet
				ll := ll.WithSubScope("stream")
				t.OnSessionQueryStreamExecute = func(
					info trace.TableSessionQueryStreamExecuteStartInfo,
				) func(
					trace.TableSessionQueryStreamExecuteIntermediateInfo,
				) func(
					trace.TableSessionQueryStreamExecuteDoneInfo,
				) {
					session := info.Session
					query := info.Query
					params := info.Parameters
					if options.LogQuery {
						ll.Trace(`stream execute start`,
							logs.String("id", session.ID()),
							logs.String("status", session.Status()),
							logs.Stringer("query", query),
							logs.Stringer("params", params),
						)
					} else {
						ll.Trace(`stream execute start`,
							logs.String("id", session.ID()),
							logs.String("status", session.Status()),
						)
					}
					start := time.Now()
					return func(
						info trace.TableSessionQueryStreamExecuteIntermediateInfo,
					) func(
						trace.TableSessionQueryStreamExecuteDoneInfo,
					) {
						if info.Error == nil {
							ll.Trace(`stream execute intermediate`)
						} else {
							ll.Warn(`stream execute intermediate failed`,
								logs.Error(info.Error),
								logs.String("version", meta.Version),
							)
						}
						return func(info trace.TableSessionQueryStreamExecuteDoneInfo) {
							if info.Error == nil {
								if options.LogQuery {
									ll.Debug(`stream execute done`,
										logs.Duration("latency", time.Since(start)),
										logs.String("id", session.ID()),
										logs.String("status", session.Status()),
										logs.Stringer("query", query),
										logs.Stringer("params", params),
									)
								} else {
									ll.Debug(`stream execute done`,
										logs.Duration("latency", time.Since(start)),
										logs.String("id", session.ID()),
										logs.String("status", session.Status()),
									)
								}
							} else {
								if options.LogQuery {
									ll.Error(`stream execute failed`,
										logs.Duration("latency", time.Since(start)),
										logs.String("id", session.ID()),
										logs.String("status", session.Status()),
										logs.Stringer("query", query),
										logs.Stringer("params", params),
										logs.Error(info.Error),
										logs.String("version", meta.Version),
									)
								} else {
									ll.Error(`stream execute failed`,
										logs.Duration("latency", time.Since(start)),
										logs.String("id", session.ID()),
										logs.String("status", session.Status()),
										logs.Error(info.Error),
										logs.String("version", meta.Version),
									)
								}
							}
						}
					}
				}
				t.OnSessionQueryStreamRead = func(
					info trace.TableSessionQueryStreamReadStartInfo,
				) func(
					intermediateInfo trace.TableSessionQueryStreamReadIntermediateInfo,
				) func(
					trace.TableSessionQueryStreamReadDoneInfo,
				) {
					session := info.Session
					ll.Trace(`read start`,
						logs.String("id", session.ID()),
						logs.String("status", session.Status()),
					)
					start := time.Now()
					return func(
						info trace.TableSessionQueryStreamReadIntermediateInfo,
					) func(
						trace.TableSessionQueryStreamReadDoneInfo,
					) {
						if info.Error == nil {
							ll.Trace(`intermediate`)
						} else {
							ll.Warn(`intermediate failed`,
								logs.Error(info.Error),
								logs.String("version", meta.Version),
							)
						}
						return func(info trace.TableSessionQueryStreamReadDoneInfo) {
							if info.Error == nil {
								ll.Debug(`read done`,
									logs.Duration("latency", time.Since(start)),
									logs.String("id", session.ID()),
									logs.String("status", session.Status()),
								)
							} else {
								ll.Error(`read failed`,
									logs.Duration("latency", time.Since(start)),
									logs.String("id", session.ID()),
									logs.String("status", session.Status()),
									logs.Error(info.Error),
									logs.String("version", meta.Version),
								)
							}
						}
					}
				}
			}
		}
		if details&trace.TableSessionTransactionEvents != 0 {
			//nolint:govet
			ll := ll.WithSubScope("tx")
			t.OnSessionTransactionBegin = func(
				info trace.TableSessionTransactionBeginStartInfo,
			) func(
				trace.TableSessionTransactionBeginDoneInfo,
			) {
				session := info.Session
				ll.Trace(`begin start`,
					logs.String("id", session.ID()),
					logs.String("status", session.Status()),
				)
				start := time.Now()
				return func(info trace.TableSessionTransactionBeginDoneInfo) {
					if info.Error == nil {
						ll.Debug(`begin done`,
							logs.Duration("latency", time.Since(start)),
							logs.String("id", session.ID()),
							logs.String("status", session.Status()),
							logs.String("tx", info.Tx.ID()),
						)
					} else {
						ll.Warn(`begin failed`,
							logs.Duration("latency", time.Since(start)),
							logs.String("id", session.ID()),
							logs.String("status", session.Status()),
							logs.Error(info.Error),
							logs.String("version", meta.Version),
						)
					}
				}
			}
			t.OnSessionTransactionCommit = func(
				info trace.TableSessionTransactionCommitStartInfo,
			) func(
				trace.TableSessionTransactionCommitDoneInfo,
			) {
				session := info.Session
				tx := info.Tx
				ll.Trace(`commit start`,
					logs.String("id", session.ID()),
					logs.String("status", session.Status()),
					logs.String("tx", info.Tx.ID()),
				)
				start := time.Now()
				return func(info trace.TableSessionTransactionCommitDoneInfo) {
					if info.Error == nil {
						ll.Debug(`commit done`,
							logs.Duration("latency", time.Since(start)),
							logs.String("id", session.ID()),
							logs.String("status", session.Status()),
							logs.String("tx", tx.ID()),
						)
					} else {
						ll.Error(`commit failed`,
							logs.Duration("latency", time.Since(start)),
							logs.String("id", session.ID()),
							logs.String("status", session.Status()),
							logs.String("tx", tx.ID()),
							logs.Error(info.Error),
							logs.String("version", meta.Version),
						)
					}
				}
			}
			t.OnSessionTransactionRollback = func(
				info trace.TableSessionTransactionRollbackStartInfo,
			) func(
				trace.TableSessionTransactionRollbackDoneInfo,
			) {
				session := info.Session
				tx := info.Tx
				ll.Trace(`rollback start`,
					logs.String("id", session.ID()),
					logs.String("status", session.Status()),
					logs.String("tx", tx.ID()),
				)
				start := time.Now()
				return func(info trace.TableSessionTransactionRollbackDoneInfo) {
					if info.Error == nil {
						ll.Debug(`rollback done`,
							logs.Duration("latency", time.Since(start)),
							logs.String("id", session.ID()),
							logs.String("status", session.Status()),
							logs.String("tx", tx.ID()),
						)
					} else {
						ll.Error(`rollback failed`,
							logs.Duration("latency", time.Since(start)),
							logs.String("id", session.ID()),
							logs.String("status", session.Status()),
							logs.String("tx", tx.ID()),
							logs.Error(info.Error),
							logs.String("version", meta.Version),
						)
					}
				}
			}
		}
	}
	//nolint:nestif
	if details&trace.TablePoolEvents != 0 {
		//nolint:govet
		ll := ll.WithSubScope("client")
		if details&trace.TablePoolLifeCycleEvents != 0 {
			t.OnInit = func(info trace.TableInitStartInfo) func(trace.TableInitDoneInfo) {
				ll.Info(`initialize start`)
				start := time.Now()
				return func(info trace.TableInitDoneInfo) {
					ll.Info(`initialize done {latency:"%v",size:{max:%d}}`,
						logs.Duration("latency", time.Since(start)),
						logs.Int("size_max", info.Limit),
					)
				}
			}
			t.OnClose = func(info trace.TableCloseStartInfo) func(trace.TableCloseDoneInfo) {
				ll.Info(`close start`)
				start := time.Now()
				return func(info trace.TableCloseDoneInfo) {
					if info.Error == nil {
						ll.Info(`close done`,
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
		}
		{
			//nolint:govet
			ll := ll.WithSubScope("pool")
			t.OnPoolStateChange = func(info trace.TablePoolStateChangeInfo) {
				ll.Info(`state changed`,
					logs.Int("size", info.Size),
					logs.String("event", info.Event),
				)
			}
		}
		if details&trace.TablePoolSessionLifeCycleEvents != 0 {
			//nolint:govet
			ll := ll.WithSubScope("session")
			t.OnPoolSessionAdd = func(info trace.TablePoolSessionAddInfo) {
				ll.Debug(`session added into pool`,
					logs.String("id", info.Session.ID()),
					logs.String("status", info.Session.Status()),
				)
			}
			t.OnPoolSessionRemove = func(info trace.TablePoolSessionRemoveInfo) {
				ll.Debug(`session removed from pool`,
					logs.String("id", info.Session.ID()),
					logs.String("status", info.Session.Status()),
				)
			}
		}
		if details&trace.TablePoolAPIEvents != 0 {
			t.OnPoolPut = func(info trace.TablePoolPutStartInfo) func(trace.TablePoolPutDoneInfo) {
				session := info.Session
				ll.Trace(`put start`,
					logs.String("id", session.ID()),
					logs.String("status", session.Status()),
				)
				start := time.Now()
				return func(info trace.TablePoolPutDoneInfo) {
					if info.Error == nil {
						ll.Trace(`put done`,
							logs.Duration("latency", time.Since(start)),
							logs.String("id", session.ID()),
							logs.String("status", session.Status()),
						)
					} else {
						ll.Error(`put failed`,
							logs.Duration("latency", time.Since(start)),
							logs.String("id", session.ID()),
							logs.String("status", session.Status()),
							logs.Error(info.Error),
							logs.String("version", meta.Version),
						)
					}
				}
			}
			t.OnPoolGet = func(info trace.TablePoolGetStartInfo) func(trace.TablePoolGetDoneInfo) {
				ll.Trace(`get start`)
				start := time.Now()
				return func(info trace.TablePoolGetDoneInfo) {
					if info.Error == nil {
						session := info.Session
						ll.Trace(`get done`,
							logs.Duration("latency", time.Since(start)),
							logs.String("id", session.ID()),
							logs.String("status", session.Status()),
							logs.Int("attempts", info.Attempts),
						)
					} else {
						ll.Warn(`get failed`,
							logs.Duration("latency", time.Since(start)),
							logs.Int("attempts", info.Attempts),
							logs.Error(info.Error),
							logs.String("version", meta.Version),
						)
					}
				}
			}
			t.OnPoolWait = func(info trace.TablePoolWaitStartInfo) func(trace.TablePoolWaitDoneInfo) {
				ll.Trace(`wait start`)
				start := time.Now()
				return func(info trace.TablePoolWaitDoneInfo) {
					if info.Error == nil {
						if info.Session == nil {
							ll.Trace(`wait done without any significant result`,
								logs.Duration("latency", time.Since(start)),
							)
						} else {
							ll.Trace(`wait done`,
								logs.Duration("latency", time.Since(start)),
								logs.String("id", info.Session.ID()),
								logs.String("status", info.Session.Status()),
							)
						}
					} else {
						if info.Session == nil {
							ll.Trace(`wait failed`,
								logs.Duration("latency", time.Since(start)),
								logs.Error(info.Error),
							)
						} else {
							ll.Trace(`wait failed with session`,
								logs.Duration("latency", time.Since(start)),
								logs.String("id", info.Session.ID()),
								logs.String("status", info.Session.Status()),
								logs.Error(info.Error),
							)
						}
					}
				}
			}
		}
	}
	return t
}
