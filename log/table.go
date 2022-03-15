package log

import (
	"errors"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Table makes trace.Table with internal logging
// nolint:gocyclo
func Table(log Logger, details trace.Details) (t trace.Table) {
	log = log.WithName(`table`)
	// nolint:nestif
	if details&trace.TablePoolRetryEvents != 0 {
		// nolint:govet
		log := log.WithName(`retry`)
		t.OnDo = func(
			info trace.TableDoStartInfo,
		) func(
			info trace.TableDoIntermediateInfo,
		) func(
			trace.TableDoDoneInfo,
		) {
			idempotent := info.Idempotent
			log.Tracef(`do start {idempotent:%t}`,
				idempotent,
			)
			start := time.Now()
			return func(info trace.TableDoIntermediateInfo) func(trace.TableDoDoneInfo) {
				if info.Error == nil {
					log.Tracef(`do intermediate {latency:"%v",idempotent:%t}`,
						time.Since(start),
						idempotent,
					)
				} else {
					if m := LevelMapper(nil); errors.As(info.Error, &m) {
						logf(
							log,
							m.MapLogLevel(DEBUG),
							`do intermediate {latency:"%v",idempotent:%t,error:"%v"}`,
							time.Since(start),
							idempotent,
							info.Error,
						)
					} else {
						log.Debugf(`do intermediate {latency:"%v",idempotent:%t,error:"%v"}`,
							time.Since(start),
							idempotent,
							info.Error,
						)
					}
				}
				return func(info trace.TableDoDoneInfo) {
					if info.Error == nil {
						log.Tracef(`do done {latency:"%v",idempotent:%t,attempts:%d}`,
							time.Since(start),
							idempotent,
							info.Attempts,
						)
					} else {
						if m := LevelMapper(nil); errors.As(info.Error, &m) {
							logf(
								log,
								m.MapLogLevel(ERROR),
								`do failed {latency:"%v",idempotent:%t,attempts:%d,error:"%v"}`,
								time.Since(start),
								idempotent,
								info.Attempts,
								info.Error,
							)
						} else {
							log.Errorf(`do intermediate {latency:"%v",idempotent:%t,error:"%v"}`,
								time.Since(start),
								idempotent,
								info.Error,
							)
						}
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
			log.Tracef(`doTx start {idempotent:%t}`,
				idempotent,
			)
			start := time.Now()
			return func(info trace.TableDoTxIntermediateInfo) func(trace.TableDoTxDoneInfo) {
				if info.Error == nil {
					log.Tracef(`doTx intermediate {latency:"%v",idempotent:%t}`,
						time.Since(start),
						idempotent,
					)
				} else {
					if m := LevelMapper(nil); errors.As(info.Error, &m) {
						logf(
							log,
							m.MapLogLevel(DEBUG),
							`doTx intermediate {latency:"%v",idempotent:%t,error:"%v"}`,
							time.Since(start),
							idempotent,
							info.Error,
						)
					} else {
						log.Errorf(`doTx intermediate {latency:"%v",idempotent:%t,error:"%v"}`,
							time.Since(start),
							idempotent,
							info.Error,
						)
					}
				}
				return func(info trace.TableDoTxDoneInfo) {
					if info.Error == nil {
						log.Tracef(`doTx done {latency:"%v",idempotent:%t,attempts:%d}`,
							time.Since(start),
							idempotent,
							info.Attempts,
						)
					} else {
						if m := LevelMapper(nil); errors.As(info.Error, &m) {
							logf(
								log,
								m.MapLogLevel(ERROR),
								`doTx failed {latency:"%v",idempotent:%t,attempts:%d,error:"%v"}`,
								time.Since(start),
								idempotent,
								info.Attempts,
								info.Error,
							)
						} else {
							log.Errorf(`doTx failed {latency:"%v",idempotent:%t,attempts:%d,error:"%v"}`,
								time.Since(start),
								idempotent,
								info.Attempts,
								info.Error,
							)
						}
					}
				}
			}
		}
	}
	// nolint:nestif
	if details&trace.TableSessionEvents != 0 {
		// nolint:govet
		log := log.WithName(`session`)
		if details&trace.TableSessionLifeCycleEvents != 0 {
			t.OnSessionNew = func(info trace.TableSessionNewStartInfo) func(trace.TableSessionNewDoneInfo) {
				log.Tracef(`create start`)
				start := time.Now()
				return func(info trace.TableSessionNewDoneInfo) {
					if info.Error == nil {
						if info.Session != nil {
							log.Tracef(`create done {latency:"%v",id:%d}`,
								time.Since(start),
								info.Session.ID(),
							)
						} else {
							log.Warnf(`create done without session {latency:"%v"}`,
								time.Since(start),
							)
						}
					} else {
						log.Warnf(`create failed {latency:"%v",error:"%v"}`,
							time.Since(start),
							info.Error,
						)
					}
				}
			}
			t.OnSessionDelete = func(info trace.TableSessionDeleteStartInfo) func(trace.TableSessionDeleteDoneInfo) {
				session := info.Session
				log.Tracef(`delete start {id:"%s",status:"%s"}`,
					session.ID(),
					session.Status(),
				)
				start := time.Now()
				return func(info trace.TableSessionDeleteDoneInfo) {
					if info.Error == nil {
						log.Tracef(`delete done {latency:"%v",id:"%s",status:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
						)
					} else {
						log.Warnf(`delete failed {latency:"%v",id:"%s",status:"%s",error:"%v"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							info.Error,
						)
					}
				}
			}
			t.OnSessionKeepAlive = func(info trace.TableKeepAliveStartInfo) func(trace.TableKeepAliveDoneInfo) {
				session := info.Session
				log.Tracef(`keep-alive start {id:"%s",status:"%s"}`,
					session.ID(),
					session.Status(),
				)
				start := time.Now()
				return func(info trace.TableKeepAliveDoneInfo) {
					if info.Error == nil {
						log.Tracef(`keep-alive done {latency:"%v",id:"%s",status:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
						)
					} else {
						log.Warnf(`keep-alive failed {latency:"%v",id:"%s",status:"%s",error:"%v"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							info.Error,
						)
					}
				}
			}
		}
		if details&trace.TableSessionQueryEvents != 0 {
			// nolint:govet
			log := log.WithName(`query`)
			if details&trace.TableSessionQueryInvokeEvents != 0 {
				// nolint:govet
				log := log.WithName(`invoke`)
				t.OnSessionQueryPrepare = func(
					info trace.TablePrepareDataQueryStartInfo,
				) func(
					trace.TablePrepareDataQueryDoneInfo,
				) {
					session := info.Session
					query := info.Query
					log.Tracef(`prepare start {id:"%s",status:"%s",query:"%s"}`,
						session.ID(),
						session.Status(),
						query,
					)
					start := time.Now()
					return func(info trace.TablePrepareDataQueryDoneInfo) {
						if info.Error == nil {
							log.Debugf(`prepare done {latency:"%v",id:"%s",status:"%s",query:"%s",result:"%s"}`,
								time.Since(start),
								session.ID(),
								session.Status(),
								query,
								info.Result,
							)
						} else {
							log.Errorf(`prepare failed {latency:"%v",id:"%s",status:"%s",query:"%s",error:"%v"}`,
								time.Since(start),
								session.ID(),
								session.Status(),
								query,
								info.Error,
							)
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
					log.Tracef(`execute start {id:"%s",status:"%s",query:"%s",params:"%s"}`,
						session.ID(),
						session.Status(),
						query,
						params,
					)
					start := time.Now()
					return func(info trace.TableExecuteDataQueryDoneInfo) {
						if info.Error == nil {
							tx := info.Tx
							log.Debugf(
								`execute done {latency:"%v",id:"%s",status:"%s",tx:"%s",query:"%s",params:"%s",prepared:%t,result:{err:"%v"}}`,
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
							log.Errorf(`execute failed {latency:"%v",id:"%s",status:"%s",query:"%s",params:"%s",prepared:%t,error:"%v"}`,
								time.Since(start),
								session.ID(),
								session.Status(),
								query,
								params,
								info.Prepared,
								info.Error,
							)
						}
					}
				}
			}
			if details&trace.TableSessionQueryStreamEvents != 0 {
				// nolint:govet
				log := log.WithName(`stream`)
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
					log.Tracef(`stream execute start {id:"%s",status:"%s",query:"%s",params:"%s"}`,
						session.ID(),
						session.Status(),
						query,
						params,
					)
					start := time.Now()
					return func(
						info trace.TableSessionQueryStreamExecuteIntermediateInfo,
					) func(
						trace.TableSessionQueryStreamExecuteDoneInfo,
					) {
						if info.Error == nil {
							log.Tracef(`stream execute intermediate`)
						} else {
							log.Warnf(`stream execute intermediate failed {error:"%v"}`,
								info.Error,
							)
						}
						return func(info trace.TableSessionQueryStreamExecuteDoneInfo) {
							if info.Error == nil {
								log.Debugf(`stream execute done {latency:"%v",id:"%s",status:"%s",query:"%s",params:"%s"}`,
									time.Since(start),
									session.ID(),
									session.Status(),
									query,
									params,
								)
							} else {
								log.Errorf(`stream execute failed {latency:"%v",id:"%s",status:"%s",query:"%s",params:"%s",error:"%v"}`,
									time.Since(start),
									session.ID(),
									session.Status(),
									query,
									params,
									info.Error,
								)
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
					log.Tracef(`read start {id:"%s",status:"%s"}`,
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
							log.Tracef(`intermediate`)
						} else {
							log.Warnf(`intermediate failed {error:"%v"}`,
								info.Error,
							)
						}
						return func(info trace.TableSessionQueryStreamReadDoneInfo) {
							if info.Error == nil {
								log.Debugf(`read done {latency:"%v",id:"%s",status:"%s"}`,
									time.Since(start),
									session.ID(),
									session.Status(),
								)
							} else {
								log.Errorf(`read failed {latency:"%v",id:"%s",status:"%s",error:"%v"}`,
									time.Since(start),
									session.ID(),
									session.Status(),
									info.Error,
								)
							}
						}
					}
				}
			}
		}
		if details&trace.TableSessionTransactionEvents != 0 {
			// nolint:govet
			log := log.WithName(`transaction`)
			t.OnSessionTransactionBegin = func(
				info trace.TableSessionTransactionBeginStartInfo,
			) func(
				trace.TableSessionTransactionBeginDoneInfo,
			) {
				session := info.Session
				log.Tracef(`begin start {id:"%s",status:"%s"}`,
					session.ID(),
					session.Status(),
				)
				start := time.Now()
				return func(info trace.TableSessionTransactionBeginDoneInfo) {
					if info.Error == nil {
						log.Debugf(`begin done {latency:"%v",id:"%s",status:"%s",tx:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							info.Tx.ID(),
						)
					} else {
						log.Warnf(`begin failed {latency:"%v",id:"%s",status:"%s",error:"%v"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							info.Error,
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
				log.Tracef(`commit start {id:"%s",status:"%s",tx:"%s"}`,
					session.ID(),
					session.Status(),
					tx.ID(),
				)
				start := time.Now()
				return func(info trace.TableSessionTransactionCommitDoneInfo) {
					if info.Error == nil {
						log.Debugf(`commit done {latency:"%v",id:"%s",status:"%s",tx:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							tx.ID(),
						)
					} else {
						log.Errorf(`commit failed {latency:"%v",id:"%s",status:"%s",tx:"%s",error:"%v"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							tx.ID(),
							info.Error,
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
				log.Tracef(`rollback start {id:"%s",status:"%s",tx:"%s"}`,
					session.ID(),
					session.Status(),
					tx.ID(),
				)
				start := time.Now()
				return func(info trace.TableSessionTransactionRollbackDoneInfo) {
					if info.Error == nil {
						log.Debugf(`rollback done {latency:"%v",id:"%s",status:"%s",tx:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							tx.ID(),
						)
					} else {
						log.Errorf(`rollback failed {latency:"%v",id:"%s",status:"%s",tx:"%s",error:"%v"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							tx.ID(),
							info.Error,
						)
					}
				}
			}
		}
	}
	// nolint:nestif
	if details&trace.TablePoolEvents != 0 {
		// nolint:govet
		log := log.WithName(`pool`)
		if details&trace.TablePoolLifeCycleEvents != 0 {
			t.OnInit = func(info trace.TableInitStartInfo) func(trace.TableInitDoneInfo) {
				log.Infof(`initialize start`)
				start := time.Now()
				return func(info trace.TableInitDoneInfo) {
					log.Infof(`initialize done {latency:"%v",size:{min:%d,max:%d}}`,
						time.Since(start),
						info.KeepAliveMinSize,
						info.Limit,
					)
				}
			}
			t.OnClose = func(info trace.TableCloseStartInfo) func(trace.TableCloseDoneInfo) {
				log.Infof(`close start`)
				start := time.Now()
				return func(info trace.TableCloseDoneInfo) {
					if info.Error == nil {
						log.Infof(`close done {latency:"%v"}`,
							time.Since(start),
						)
					} else {
						log.Errorf(`close failed {latency:"%v",error:"%v"}`,
							time.Since(start),
							info.Error,
						)
					}
				}
			}
			t.OnPoolStateChange = func(info trace.TablePooStateChangeInfo) {
				log.Infof(`state changed {size:%d,event:"%s"}`, info.Size, info.Event)
			}
		}
		if details&trace.TablePoolSessionLifeCycleEvents != 0 {
			// nolint:govet
			log := log.WithName(`session`)
			t.OnPoolSessionNew = func(info trace.TablePoolSessionNewStartInfo) func(trace.TablePoolSessionNewDoneInfo) {
				log.Tracef(`create start`)
				start := time.Now()
				return func(info trace.TablePoolSessionNewDoneInfo) {
					if info.Error == nil {
						session := info.Session
						if session != nil {
							log.Debugf(`create done {latency:"%v",id:"%s",status:"%s"}`,
								time.Since(start),
								session.ID(),
								session.Status(),
							)
						}
					} else {
						log.Errorf(`create failed {latency:"%v",error:"%v"}`,
							time.Since(start),
							info.Error,
						)
					}
				}
			}
			t.OnPoolSessionClose = func(info trace.TablePoolSessionCloseStartInfo) func(trace.TablePoolSessionCloseDoneInfo) {
				session := info.Session
				log.Tracef(`close start {id:"%s",status:"%s"}`,
					session.ID(),
					session.Status(),
				)
				start := time.Now()
				return func(info trace.TablePoolSessionCloseDoneInfo) {
					log.Debugf(`close done {latency:"%v",id:"%s",status:"%s"}`,
						time.Since(start),
						session.ID(),
						session.Status(),
					)
				}
			}
		}
		if details&trace.TablePoolAPIEvents != 0 {
			t.OnPoolPut = func(info trace.TablePoolPutStartInfo) func(trace.TablePoolPutDoneInfo) {
				session := info.Session
				log.Tracef(`put start {id:"%s",status:"%s"}`,
					session.ID(),
					session.Status(),
				)
				start := time.Now()
				return func(info trace.TablePoolPutDoneInfo) {
					if info.Error == nil {
						log.Tracef(`put done {latency:"%v",id:"%s",status:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
						)
					} else {
						log.Errorf(`put failed {latency:"%v",id:"%s",status:"%s",error:"%v"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							info.Error,
						)
					}
				}
			}
			t.OnPoolGet = func(info trace.TablePoolGetStartInfo) func(trace.TablePoolGetDoneInfo) {
				log.Tracef(`get start`)
				start := time.Now()
				return func(info trace.TablePoolGetDoneInfo) {
					if info.Error == nil {
						session := info.Session
						log.Tracef(`get done {latency:"%v",id:"%s",status:"%s",attempts:%d}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							info.Attempts,
						)
					} else {
						log.Warnf(`get failed {latency:"%v",attempts:%d,error:"%v"}`,
							time.Since(start),
							info.Attempts,
							info.Error,
						)
					}
				}
			}
			t.OnPoolWait = func(info trace.TablePoolWaitStartInfo) func(trace.TablePoolWaitDoneInfo) {
				log.Tracef(`wait start`)
				start := time.Now()
				return func(info trace.TablePoolWaitDoneInfo) {
					if info.Error == nil {
						if info.Session == nil {
							log.Tracef(`wait done without any significant result {latency:"%v"}`,
								time.Since(start),
							)
						} else {
							log.Tracef(`wait done {latency:"%v",id:"%s",status:"%s"}`,
								time.Since(start),
								info.Session.ID(),
								info.Session.Status(),
							)
						}
					} else {
						if info.Session == nil {
							log.Tracef(`wait failed {latency:"%v",error:"%v"}`,
								time.Since(start),
								info.Error,
							)
						} else {
							log.Tracef(`wait failed with session {latency:"%v",id:"%s",status:"%s",error:"%v"}`,
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
