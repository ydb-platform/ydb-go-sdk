package log

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Table makes trace.Table with logging events from details
//
//nolint:gocyclo
func Table(l Logger, details trace.Details, opts ...option) (t trace.Table) {
	if details&trace.TableEvents == 0 {
		return
	}
	options := parseOptions(opts...)
	l = l.WithName(`table`)
	t.OnDo = func(
		info trace.TableDoStartInfo,
	) func(
		info trace.TableDoIntermediateInfo,
	) func(
		trace.TableDoDoneInfo,
	) {
		idempotent := info.Idempotent
		l.Tracef(`do start {idempotent:%t}`,
			idempotent,
		)
		start := time.Now()
		return func(info trace.TableDoIntermediateInfo) func(trace.TableDoDoneInfo) {
			if info.Error == nil {
				l.Tracef(`do attempt done {latency:"%v",idempotent:%t}`,
					time.Since(start),
					idempotent,
				)
			} else {
				f := l.Warnf
				if !xerrors.IsYdb(info.Error) {
					f = l.Debugf
				}
				m := retry.Check(info.Error)
				f(`do attempt failed {latency:"%v",idempotent:%t,error:"%s",retryable:%t,code:%d,deleteSession:%t,version:"%s"}`,
					time.Since(start),
					idempotent,
					info.Error,
					m.MustRetry(idempotent),
					m.StatusCode(),
					m.MustDeleteSession(),
					meta.Version,
				)
			}
			return func(info trace.TableDoDoneInfo) {
				if info.Error == nil {
					l.Tracef(`do done {latency:"%v",idempotent:%t,attempts:%d}`,
						time.Since(start),
						idempotent,
						info.Attempts,
					)
				} else {
					f := l.Errorf
					if !xerrors.IsYdb(info.Error) {
						f = l.Debugf
					}
					m := retry.Check(info.Error)
					f(`do failed {latency:"%v",idempotent:%t,attempts:%d,error:"%s",retryable:%t,code:%d,deleteSession:%t}`,
						time.Since(start),
						idempotent,
						info.Attempts,
						info.Error,
						m.MustRetry(idempotent),
						m.StatusCode(),
						m.MustDeleteSession(),
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
		l.Tracef(`doTx start {idempotent:%t}`,
			idempotent,
		)
		start := time.Now()
		return func(info trace.TableDoTxIntermediateInfo) func(trace.TableDoTxDoneInfo) {
			if info.Error == nil {
				l.Tracef(`doTx attempt done {latency:"%v",idempotent:%t}`,
					time.Since(start),
					idempotent,
				)
			} else {
				f := l.Warnf
				if !xerrors.IsYdb(info.Error) {
					f = l.Debugf
				}
				m := retry.Check(info.Error)
				f(`doTx attempt failed {latency:"%v",idempotent:%t,error:"%s",retryable:%t,code:%d,deleteSession:%t,version:"%s"}`,
					time.Since(start),
					idempotent,
					info.Error,
					m.MustRetry(idempotent),
					m.StatusCode(),
					m.MustDeleteSession(),
					meta.Version,
				)
			}
			return func(info trace.TableDoTxDoneInfo) {
				if info.Error == nil {
					l.Tracef(`doTx done {latency:"%v",idempotent:%t,attempts:%d}`,
						time.Since(start),
						idempotent,
						info.Attempts,
					)
				} else {
					f := l.Warnf
					if !xerrors.IsYdb(info.Error) {
						f = l.Debugf
					}
					m := retry.Check(info.Error)
					//nolint:lll
					f(`doTx failed {latency:"%v",idempotent:%t,attempts:%d,error:"%s",retryable:%t,code:%d,deleteSession:%t,version:"%s"}`,
						time.Since(start),
						idempotent,
						info.Attempts,
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
	t.OnCreateSession = func(
		info trace.TableCreateSessionStartInfo,
	) func(
		info trace.TableCreateSessionIntermediateInfo,
	) func(
		trace.TableCreateSessionDoneInfo,
	) {
		l.Tracef(`create session start`)
		start := time.Now()
		return func(info trace.TableCreateSessionIntermediateInfo) func(trace.TableCreateSessionDoneInfo) {
			if info.Error == nil {
				l.Tracef(`create session intermediate {latency:"%v"}`,
					time.Since(start),
				)
			} else {
				l.Errorf(`create session intermediate {latency:"%v",error:"%v",version:"%s"}`,
					time.Since(start),
					info.Error,
					meta.Version,
				)
			}
			return func(info trace.TableCreateSessionDoneInfo) {
				if info.Error == nil {
					l.Tracef(`create session done {latency:"%v",attempts:%d,session:{id:"%s",status:"%s"}}`,
						time.Since(start),
						info.Attempts,
						info.Session.ID(),
						info.Session.Status(),
					)
				} else {
					l.Errorf(`create session failed {latency:"%v",attempts:%d,error:"%v",version:"%s"}`,
						time.Since(start),
						info.Attempts,
						info.Error,
						meta.Version,
					)
				}
			}
		}
	}
	//nolint:nestif
	if details&trace.TableSessionEvents != 0 {
		//nolint:govet
		l := l.WithName(`session`)
		if details&trace.TableSessionLifeCycleEvents != 0 {
			t.OnSessionNew = func(info trace.TableSessionNewStartInfo) func(trace.TableSessionNewDoneInfo) {
				l.Tracef(`create start`)
				start := time.Now()
				return func(info trace.TableSessionNewDoneInfo) {
					if info.Error == nil {
						if info.Session != nil {
							l.Tracef(`create done {latency:"%v",id:%d}`,
								time.Since(start),
								info.Session.ID(),
							)
						} else {
							l.Warnf(`create done without session {latency:"%v",version:"%s"}`,
								time.Since(start),
								meta.Version,
							)
						}
					} else {
						l.Warnf(`create failed {latency:"%v",error:"%v",version:"%s"}`,
							time.Since(start),
							info.Error,
							meta.Version,
						)
					}
				}
			}
			t.OnSessionDelete = func(info trace.TableSessionDeleteStartInfo) func(trace.TableSessionDeleteDoneInfo) {
				session := info.Session
				l.Tracef(`delete start {id:"%s",status:"%s"}`,
					session.ID(),
					session.Status(),
				)
				start := time.Now()
				return func(info trace.TableSessionDeleteDoneInfo) {
					if info.Error == nil {
						l.Tracef(`delete done {latency:"%v",id:"%s",status:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
						)
					} else {
						l.Warnf(`delete failed {latency:"%v",id:"%s",status:"%s",error:"%v",version:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							info.Error,
							meta.Version,
						)
					}
				}
			}
			t.OnSessionKeepAlive = func(info trace.TableKeepAliveStartInfo) func(trace.TableKeepAliveDoneInfo) {
				session := info.Session
				l.Tracef(`keep-alive start {id:"%s",status:"%s"}`,
					session.ID(),
					session.Status(),
				)
				start := time.Now()
				return func(info trace.TableKeepAliveDoneInfo) {
					if info.Error == nil {
						l.Tracef(`keep-alive done {latency:"%v",id:"%s",status:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
						)
					} else {
						l.Warnf(`keep-alive failed {latency:"%v",id:"%s",status:"%s",error:"%v",version:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							info.Error,
							meta.Version,
						)
					}
				}
			}
		}
		if details&trace.TableSessionQueryEvents != 0 {
			//nolint:govet
			l := l.WithName(`query`)
			if details&trace.TableSessionQueryInvokeEvents != 0 {
				//nolint:govet
				l := l.WithName(`invoke`)
				t.OnSessionQueryPrepare = func(
					info trace.TablePrepareDataQueryStartInfo,
				) func(
					trace.TablePrepareDataQueryDoneInfo,
				) {
					session := info.Session
					query := info.Query
					if options.logQuery {
						l.Tracef(`prepare start {id:"%s",status:"%s",query:"%s"}`,
							session.ID(),
							session.Status(),
							query,
						)
					} else {
						l.Tracef(`prepare start {id:"%s",status:"%s"}`,
							session.ID(),
							session.Status(),
						)
					}
					start := time.Now()
					return func(info trace.TablePrepareDataQueryDoneInfo) {
						if info.Error == nil {
							if options.logQuery {
								l.Debugf(`prepare done {latency:"%v",id:"%s",status:"%s",query:"%s",result:"%s"}`,
									time.Since(start),
									session.ID(),
									session.Status(),
									query,
									info.Result,
								)
							} else {
								l.Debugf(`prepare done {latency:"%v",id:"%s",status:"%s",result:"%s"}`,
									time.Since(start),
									session.ID(),
									session.Status(),
									info.Result,
								)
							}
						} else {
							if options.logQuery {
								l.Errorf(`prepare failed {latency:"%v",id:"%s",status:"%s",query:"%s",error:"%v",version:"%s"}`,
									time.Since(start),
									session.ID(),
									session.Status(),
									query,
									info.Error,
									meta.Version,
								)
							} else {
								l.Errorf(`prepare failed {latency:"%v",id:"%s",status:"%s",error:"%v",version:"%s"}`,
									time.Since(start),
									session.ID(),
									session.Status(),
									info.Error,
									meta.Version,
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
					keepInCache := info.KeepInCache
					if options.logQuery {
						l.Tracef(`execute start {id:"%s",status:"%s",query:"%s",params:"%s",keepInCache:%v}`,
							session.ID(),
							session.Status(),
							query,
							params,
							keepInCache,
						)
					} else {
						l.Tracef(`execute start {id:"%s",status:"%s"}`,
							session.ID(),
							session.Status(),
						)
					}
					start := time.Now()
					return func(info trace.TableExecuteDataQueryDoneInfo) {
						if info.Error == nil {
							tx := info.Tx
							if options.logQuery {
								//nolint:lll
								l.Debugf(`execute done {latency:"%v",id:"%s",status:"%s",tx:"%s",query:"%s",params:"%s",prepared:%t,result:{err:"%v"}}`,
									time.Since(start),
									session.ID(),
									session.Status(),
									tx.ID(),
									query,
									params,
									info.Prepared,
									info.Result.Err(),
								)
							} else {
								l.Debugf(`execute done {latency:"%v",id:"%s",status:"%s",tx:"%s",prepared:%t,result:{err:"%v"}}`,
									time.Since(start),
									session.ID(),
									session.Status(),
									tx.ID(),
									info.Prepared,
									info.Result.Err(),
								)
							}
						} else {
							if options.logQuery {
								//nolint:lll
								l.Errorf(`execute failed {latency:"%v",id:"%s",status:"%s",query:"%s",params:"%s",prepared:%t,error:"%v",version:"%s"}`,
									time.Since(start),
									session.ID(),
									session.Status(),
									query,
									params,
									info.Prepared,
									info.Error,
									meta.Version,
								)
							} else {
								l.Errorf(`execute failed {latency:"%v",id:"%s",status:"%s",prepared:%t,error:"%v",version:"%s"}`,
									time.Since(start),
									session.ID(),
									session.Status(),
									info.Prepared,
									info.Error,
									meta.Version,
								)
							}
						}
					}
				}
			}
			if details&trace.TableSessionQueryStreamEvents != 0 {
				//nolint:govet
				l := l.WithName(`stream`)
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
					if options.logQuery {
						l.Tracef(`stream execute start {id:"%s",status:"%s",query:"%s",params:"%s"}`,
							session.ID(),
							session.Status(),
							query,
							params,
						)
					} else {
						l.Tracef(`stream execute start {id:"%s",status:"%s"}`,
							session.ID(),
							session.Status(),
						)
					}
					start := time.Now()
					return func(
						info trace.TableSessionQueryStreamExecuteIntermediateInfo,
					) func(
						trace.TableSessionQueryStreamExecuteDoneInfo,
					) {
						if info.Error == nil {
							l.Tracef(`stream execute intermediate`)
						} else {
							l.Warnf(`stream execute intermediate failed {error:"%v",version:"%s"}`,
								info.Error,
								meta.Version,
							)
						}
						return func(info trace.TableSessionQueryStreamExecuteDoneInfo) {
							if info.Error == nil {
								if options.logQuery {
									l.Debugf(`stream execute done {latency:"%v",id:"%s",status:"%s",query:"%s",params:"%s"}`,
										time.Since(start),
										session.ID(),
										session.Status(),
										query,
										params,
									)
								} else {
									l.Debugf(`stream execute done {latency:"%v",id:"%s",status:"%s"}`,
										time.Since(start),
										session.ID(),
										session.Status(),
									)
								}
							} else {
								if options.logQuery {
									//nolint:lll
									l.Errorf(`stream execute failed {latency:"%v",id:"%s",status:"%s",query:"%s",params:"%s",error:"%v",version:"%s"}`,
										time.Since(start),
										session.ID(),
										session.Status(),
										query,
										params,
										info.Error,
										meta.Version,
									)
								} else {
									l.Errorf(`stream execute failed {latency:"%v",id:"%s",status:"%s",error:"%v",version:"%s"}`,
										time.Since(start),
										session.ID(),
										session.Status(),
										info.Error,
										meta.Version,
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
					l.Tracef(`read start {id:"%s",status:"%s"}`,
						session.ID(),
						session.Status(),
					)
					start := time.Now()
					return func(
						info trace.TableSessionQueryStreamReadIntermediateInfo,
					) func(
						trace.TableSessionQueryStreamReadDoneInfo,
					) {
						if info.Error == nil {
							l.Tracef(`intermediate`)
						} else {
							l.Warnf(`intermediate failed {error:"%v",version:"%s"}`,
								info.Error,
								meta.Version,
							)
						}
						return func(info trace.TableSessionQueryStreamReadDoneInfo) {
							if info.Error == nil {
								l.Debugf(`read done {latency:"%v",id:"%s",status:"%s"}`,
									time.Since(start),
									session.ID(),
									session.Status(),
								)
							} else {
								l.Errorf(`read failed {latency:"%v",id:"%s",status:"%s",error:"%v",version:"%s"}`,
									time.Since(start),
									session.ID(),
									session.Status(),
									info.Error,
									meta.Version,
								)
							}
						}
					}
				}
			}
		}
		if details&trace.TableSessionTransactionEvents != 0 {
			//nolint:govet
			l := l.WithName(`tx`)
			t.OnSessionTransactionBegin = func(
				info trace.TableSessionTransactionBeginStartInfo,
			) func(
				trace.TableSessionTransactionBeginDoneInfo,
			) {
				session := info.Session
				l.Tracef(`begin start {id:"%s",status:"%s"}`,
					session.ID(),
					session.Status(),
				)
				start := time.Now()
				return func(info trace.TableSessionTransactionBeginDoneInfo) {
					if info.Error == nil {
						l.Debugf(`begin done {latency:"%v",id:"%s",status:"%s",tx:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							info.Tx.ID(),
						)
					} else {
						l.Warnf(`begin failed {latency:"%v",id:"%s",status:"%s",error:"%v",version:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							info.Error,
							meta.Version,
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
				l.Tracef(`commit start {id:"%s",status:"%s",tx:"%s"}`,
					session.ID(),
					session.Status(),
					tx.ID(),
				)
				start := time.Now()
				return func(info trace.TableSessionTransactionCommitDoneInfo) {
					if info.Error == nil {
						l.Debugf(`commit done {latency:"%v",id:"%s",status:"%s",tx:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							tx.ID(),
						)
					} else {
						l.Errorf(`commit failed {latency:"%v",id:"%s",status:"%s",tx:"%s",error:"%v",version:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							tx.ID(),
							info.Error,
							meta.Version,
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
				l.Tracef(`rollback start {id:"%s",status:"%s",tx:"%s"}`,
					session.ID(),
					session.Status(),
					tx.ID(),
				)
				start := time.Now()
				return func(info trace.TableSessionTransactionRollbackDoneInfo) {
					if info.Error == nil {
						l.Debugf(`rollback done {latency:"%v",id:"%s",status:"%s",tx:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							tx.ID(),
						)
					} else {
						l.Errorf(`rollback failed {latency:"%v",id:"%s",status:"%s",tx:"%s",error:"%v",version:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							tx.ID(),
							info.Error,
							meta.Version,
						)
					}
				}
			}
		}
	}
	//nolint:nestif
	if details&trace.TablePoolEvents != 0 {
		//nolint:govet
		l := l.WithName(`client`)
		if details&trace.TablePoolLifeCycleEvents != 0 {
			t.OnInit = func(info trace.TableInitStartInfo) func(trace.TableInitDoneInfo) {
				l.Infof(`initialize start`)
				start := time.Now()
				return func(info trace.TableInitDoneInfo) {
					l.Infof(`initialize done {latency:"%v",size:{max:%d}}`,
						time.Since(start),
						info.Limit,
					)
				}
			}
			t.OnClose = func(info trace.TableCloseStartInfo) func(trace.TableCloseDoneInfo) {
				l.Infof(`close start`)
				start := time.Now()
				return func(info trace.TableCloseDoneInfo) {
					if info.Error == nil {
						l.Infof(`close done {latency:"%v"}`,
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
		}
		{
			//nolint:govet
			l := l.WithName(`pool`)
			t.OnPoolStateChange = func(info trace.TablePoolStateChangeInfo) {
				l.Infof(`state changed {size:%d,event:"%s"}`,
					info.Size,
					info.Event,
				)
			}
		}
		if details&trace.TablePoolSessionLifeCycleEvents != 0 {
			//nolint:govet
			l := l.WithName(`session`)
			t.OnPoolSessionAdd = func(info trace.TablePoolSessionAddInfo) {
				l.Debugf(`session added into pool {id:"%s",status:"%s"}`,
					info.Session.ID(),
					info.Session.Status(),
				)
			}
			t.OnPoolSessionRemove = func(info trace.TablePoolSessionRemoveInfo) {
				l.Debugf(`session removed from pool {id:"%s",status:"%s"}`,
					info.Session.ID(),
					info.Session.Status(),
				)
			}
		}
		if details&trace.TablePoolAPIEvents != 0 {
			t.OnPoolPut = func(info trace.TablePoolPutStartInfo) func(trace.TablePoolPutDoneInfo) {
				session := info.Session
				l.Tracef(`put start {id:"%s",status:"%s"}`,
					session.ID(),
					session.Status(),
				)
				start := time.Now()
				return func(info trace.TablePoolPutDoneInfo) {
					if info.Error == nil {
						l.Tracef(`put done {latency:"%v",id:"%s",status:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
						)
					} else {
						l.Warnf(`put failed {latency:"%v",id:"%s",status:"%s",error:"%v",version:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							info.Error,
							meta.Version,
						)
					}
				}
			}
			t.OnPoolGet = func(info trace.TablePoolGetStartInfo) func(trace.TablePoolGetDoneInfo) {
				l.Tracef(`get start`)
				start := time.Now()
				return func(info trace.TablePoolGetDoneInfo) {
					if info.Error == nil {
						session := info.Session
						l.Tracef(`get done {latency:"%v",id:"%s",status:"%s",attempts:%d}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							info.Attempts,
						)
					} else {
						l.Warnf(`get failed {latency:"%v",attempts:%d,error:"%v",version:"%s"}`,
							time.Since(start),
							info.Attempts,
							info.Error,
							meta.Version,
						)
					}
				}
			}
			t.OnPoolWait = func(info trace.TablePoolWaitStartInfo) func(trace.TablePoolWaitDoneInfo) {
				l.Tracef(`wait start`)
				start := time.Now()
				return func(info trace.TablePoolWaitDoneInfo) {
					if info.Error == nil {
						if info.Session == nil {
							l.Tracef(`wait done without any significant result {latency:"%v"}`,
								time.Since(start),
							)
						} else {
							l.Tracef(`wait done {latency:"%v",id:"%s",status:"%s"}`,
								time.Since(start),
								info.Session.ID(),
								info.Session.Status(),
							)
						}
					} else {
						if info.Session == nil {
							l.Tracef(`wait failed {latency:"%v",error:"%v"}`,
								time.Since(start),
								info.Error,
							)
						} else {
							l.Tracef(`wait failed with session {latency:"%v",id:"%s",status:"%s",error:"%v"}`,
								time.Since(start),
								info.Session.ID(),
								info.Session.Status(),
								info.Error,
							)
						}
					}
				}
			}
		}
	}
	return t
}
