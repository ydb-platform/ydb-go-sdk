package log

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Table makes trace.Table with internal logging
func Table(log Logger, details trace.Details) trace.Table {
	log = log.WithName(`table`)
	t := trace.Table{}
	if details&trace.TablePoolRetryEvents != 0 {
		//nolint: govet
		log := log.WithName(`retry`)
		t.OnPoolRetry = func(info trace.PoolRetryStartInfo) func(info trace.PoolRetryInternalInfo) func(trace.PoolRetryDoneInfo) {
			idempotent := info.Idempotent
			log.Tracef(`retry start {idempotent:%t}`,
				idempotent,
			)
			start := time.Now()
			return func(info trace.PoolRetryInternalInfo) func(trace.PoolRetryDoneInfo) {
				if info.Error == nil {
					log.Tracef(`retry intermediate {latency:"%s",idempotent:%t}`,
						time.Since(start),
						idempotent,
					)
				} else {
					log.Debugf(`retry intermediate {latency:"%s",idempotent:%t,error:"%s"}`,
						time.Since(start),
						idempotent,
						info.Error,
					)
				}
				return func(info trace.PoolRetryDoneInfo) {
					if info.Error == nil {
						log.Tracef(`retry done {latency:"%s",idempotent:%t,attempts:%d}`,
							time.Since(start),
							idempotent,
							info.Attempts,
						)
					} else {
						log.Errorf(`retry failed {latency:"%s",idempotent:%t,attempts:%d,error:"%s"}`,
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
	if details&trace.TableSessionEvents != 0 {
		//nolint: govet
		log := log.WithName(`session`)
		if details&trace.TableSessionLifeCycleEvents != 0 {
			t.OnSessionNew = func(info trace.SessionNewStartInfo) func(trace.SessionNewDoneInfo) {
				log.Tracef(`create start`)
				start := time.Now()
				return func(info trace.SessionNewDoneInfo) {
					if info.Error == nil {
						log.Tracef(`create done {latency:"%s",id:%d}`,
							time.Since(start),
							info.Session.ID(),
						)
					} else {
						log.Warnf(`create failed {latency:"%s",error:"%s"}`,
							time.Since(start),
							info.Error,
						)
					}
				}
			}
			t.OnSessionDelete = func(info trace.SessionDeleteStartInfo) func(trace.SessionDeleteDoneInfo) {
				session := info.Session
				log.Tracef(`delete start {id:"%s",status:"%s"}`,
					session.ID(),
					session.Status(),
				)
				start := time.Now()
				return func(info trace.SessionDeleteDoneInfo) {
					if info.Error == nil {
						log.Tracef(`delete done {latency:"%s",id:"%s",status:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
						)
					} else {
						log.Warnf(`delete failed {latency:"%s",id:"%s",status:"%s",error:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							info.Error,
						)
					}
				}
			}
			t.OnSessionKeepAlive = func(info trace.KeepAliveStartInfo) func(trace.KeepAliveDoneInfo) {
				session := info.Session
				log.Tracef(`keep-alive start {id:"%s",status:"%s"}`,
					session.ID(),
					session.Status(),
				)
				start := time.Now()
				return func(info trace.KeepAliveDoneInfo) {
					if info.Error == nil {
						log.Tracef(`keep-alive done {latency:"%s",id:"%s",status:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
						)
					} else {
						log.Warnf(`keep-alive failed {latency:"%s",id:"%s",status:"%s",error:"%s"}`,
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
			//nolint: govet
			log := log.WithName(`query`)
			if details&trace.TableSessionQueryInvokeEvents != 0 {
				//nolint: govet
				log := log.WithName(`invoke`)
				t.OnSessionQueryPrepare = func(info trace.SessionQueryPrepareStartInfo) func(trace.PrepareDataQueryDoneInfo) {
					session := info.Session
					query := info.Query
					log.Tracef(`prepare start {id:"%s",status:"%s",query:"%s"}`,
						session.ID(),
						session.Status(),
						query,
					)
					start := time.Now()
					return func(info trace.PrepareDataQueryDoneInfo) {
						if info.Error == nil {
							log.Debugf(`prepare done {latency:"%s",id:"%s",status:"%s",query:"%s",result:"%s"}`,
								time.Since(start),
								session.ID(),
								session.Status(),
								query,
								info.Result,
							)
						} else {
							log.Errorf(`prepare failed {latency:"%s",id:"%s",status:"%s",query:"%s",error:"%s"}`,
								time.Since(start),
								session.ID(),
								session.Status(),
								query,
								info.Error,
							)
						}
					}
				}
				t.OnSessionQueryExecute = func(info trace.ExecuteDataQueryStartInfo) func(trace.SessionQueryPrepareDoneInfo) {
					session := info.Session
					query := info.Query
					tx := info.Tx
					params := info.Parameters
					log.Tracef(`execute start {id:"%s",status:"%s",tx:"%s",query:"%s",params:"%s"}`,
						session.ID(),
						session.Status(),
						tx.ID(),
						query,
						params,
					)
					start := time.Now()
					return func(info trace.SessionQueryPrepareDoneInfo) {
						if info.Error == nil {
							log.Debugf(`execute done {latency:"%s",id:"%s",status:"%s",tx:"%s",query:"%s",params:"%s",prepared:%t,result:{err:"%s"}}`,
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
							log.Errorf(`execute failed {latency:"%s",id:"%s",status:"%s",tx:"%s",query:"%s",params:"%s",prepared:%t,error:"%s"}`,
								time.Since(start),
								session.ID(),
								session.Status(),
								tx.ID(),
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
				//nolint: govet
				log := log.WithName(`stream`)
				t.OnSessionQueryStreamExecute = func(info trace.SessionQueryStreamExecuteStartInfo) func(trace.SessionQueryStreamExecuteDoneInfo) {
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
					return func(info trace.SessionQueryStreamExecuteDoneInfo) {
						if info.Error == nil {
							log.Debugf(`execute done {latency:"%s",id:"%s",status:"%s",query:"%s",params:"%s",result:{err:"%s"}}`,
								time.Since(start),
								session.ID(),
								session.Status(),
								query,
								params,
								info.Result.Err(),
							)
						} else {
							log.Errorf(`execute failed {latency:"%s",id:"%s",status:"%s",query:"%s",params:"%s",error:"%s"}`,
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
				t.OnSessionQueryStreamRead = func(info trace.SessionQueryStreamReadStartInfo) func(trace.SessionQueryStreamReadDoneInfo) {
					session := info.Session
					log.Tracef(`read start {id:"%s",status:"%s"}`,
						session.ID(),
						session.Status(),
					)
					start := time.Now()
					return func(info trace.SessionQueryStreamReadDoneInfo) {
						if info.Error == nil {
							log.Debugf(`read done {latency:"%s",id:"%s",status:"%s",result:{err:"%s"}}`,
								time.Since(start),
								session.ID(),
								session.Status(),
								info.Result.Err(),
							)
						} else {
							log.Errorf(`read failed {latency:"%s",id:"%s",status:"%s",error:"%s"}`,
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
		if details&trace.TableSessionTransactionEvents != 0 {
			//nolint: govet
			log := log.WithName(`transaction`)
			t.OnSessionTransactionBegin = func(info trace.SessionTransactionBeginStartInfo) func(trace.SessionTransactionBeginDoneInfo) {
				session := info.Session
				log.Tracef(`begin start {id:"%s",status:"%s"}`,
					session.ID(),
					session.Status(),
				)
				start := time.Now()
				return func(info trace.SessionTransactionBeginDoneInfo) {
					if info.Error == nil {
						log.Debugf(`begin done {latency:"%s",id:"%s",status:"%s",tx:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							info.Tx.ID(),
						)
					} else {
						log.Warnf(`begin failed {latency:"%s",id:"%s",status:"%s",error:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							info.Error,
						)
					}
				}
			}
			t.OnSessionTransactionCommit = func(info trace.SessionTransactionCommitStartInfo) func(trace.SessionTransactionCommitDoneInfo) {
				session := info.Session
				tx := info.Tx
				log.Tracef(`commit start {id:"%s",status:"%s",tx:"%s"}`,
					session.ID(),
					session.Status(),
					tx.ID(),
				)
				start := time.Now()
				return func(info trace.SessionTransactionCommitDoneInfo) {
					if info.Error == nil {
						log.Debugf(`commit done {latency:"%s",id:"%s",status:"%s",tx:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							tx.ID(),
						)
					} else {
						log.Errorf(`commit failed {latency:"%s",id:"%s",status:"%s",tx:"%s",error:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							tx.ID(),
							info.Error,
						)
					}
				}
			}
			t.OnSessionTransactionRollback = func(info trace.SessionTransactionRollbackStartInfo) func(trace.SessionTransactionRollbackDoneInfo) {
				session := info.Session
				tx := info.Tx
				log.Tracef(`rollback start {id:"%s",status:"%s",tx:"%s"}`,
					session.ID(),
					session.Status(),
					tx.ID(),
				)
				start := time.Now()
				return func(info trace.SessionTransactionRollbackDoneInfo) {
					if info.Error == nil {
						log.Debugf(`rollback done {latency:"%s",id:"%s",status:"%s",tx:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							tx.ID(),
						)
					} else {
						log.Errorf(`rollback failed {latency:"%s",id:"%s",status:"%s",tx:"%s",error:"%s"}`,
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
	if details&trace.TablePoolEvents != 0 {
		//nolint: govet
		log := log.WithName(`pool`)
		if details&trace.TablePoolLifeCycleEvents != 0 {
			t.OnPoolInit = func(info trace.PoolInitStartInfo) func(trace.PoolInitDoneInfo) {
				log.Infof(`initialize start`)
				start := time.Now()
				return func(info trace.PoolInitDoneInfo) {
					log.Infof(`initialize done {latency:"%s",size:{min:%d,max:%d}}`,
						time.Since(start),
						info.KeepAliveMinSize,
						info.Limit,
					)
				}
			}
			t.OnPoolClose = func(info trace.PoolCloseStartInfo) func(trace.PoolCloseDoneInfo) {
				log.Infof(`close start`)
				start := time.Now()
				return func(info trace.PoolCloseDoneInfo) {
					if info.Error != nil {
						log.Infof(`close done {latency:"%s"}`,
							time.Since(start),
						)
					} else {
						log.Errorf(`close failed {latency:"%s",error:"%s"}`,
							time.Since(start),
							info.Error,
						)
					}
				}
			}
		}
		if details&trace.TablePoolSessionLifeCycleEvents != 0 {
			//nolint: govet
			log := log.WithName(`session`)
			t.OnPoolSessionNew = func(info trace.PoolSessionNewStartInfo) func(trace.PoolSessionNewDoneInfo) {
				log.Tracef(`create start`)
				start := time.Now()
				return func(info trace.PoolSessionNewDoneInfo) {
					if info.Error == nil {
						session := info.Session
						log.Debugf(`create done {latency:"%s",id:"%s",status:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
						)
					} else {
						log.Errorf(`create failed {latency:"%s",error:"%s"}`,
							time.Since(start),
							info.Error,
						)
					}
				}
			}
			t.OnPoolSessionClose = func(info trace.PoolSessionCloseStartInfo) func(trace.PoolSessionCloseDoneInfo) {
				session := info.Session
				log.Tracef(`close start {id:"%s",status:"%s"}`,
					session.ID(),
					session.Status(),
				)
				start := time.Now()
				return func(info trace.PoolSessionCloseDoneInfo) {
					log.Debugf(`close done {latency:"%s",id:"%s",status:"%s"}`,
						time.Since(start),
						session.ID(),
						session.Status(),
					)
				}
			}
		}
		if details&trace.TablePoolAPIEvents != 0 {
			t.OnPoolPut = func(info trace.PoolPutStartInfo) func(trace.PoolPutDoneInfo) {
				session := info.Session
				log.Tracef(`put start {id:"%s",status:"%s"}`,
					session.ID(),
					session.Status(),
				)
				start := time.Now()
				return func(info trace.PoolPutDoneInfo) {
					if info.Error == nil {
						log.Tracef(`put done {latency:"%s",id:"%s",status:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
						)
					} else {
						log.Errorf(`put failed {latency:"%s",id:"%s",status:"%s",error:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							info.Error,
						)
					}
				}
			}
			t.OnPoolGet = func(info trace.PoolGetStartInfo) func(trace.PoolGetDoneInfo) {
				log.Tracef(`get start`)
				start := time.Now()
				return func(info trace.PoolGetDoneInfo) {
					if info.Error == nil {
						session := info.Session
						log.Tracef(`get done {latency:"%s",id:"%s",status:"%s",attempts:%d}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							info.Attempts,
						)
					} else {
						log.Warnf(`get failed {latency:"%s",attempts:%d,error:"%s"}`,
							time.Since(start),
							info.Attempts,
							info.Error,
						)
					}
				}
			}
			t.OnPoolWait = func(info trace.PoolWaitStartInfo) func(trace.PoolWaitDoneInfo) {
				log.Tracef(`wait start`)
				start := time.Now()
				return func(info trace.PoolWaitDoneInfo) {
					if info.Error == nil {
						log.Tracef(`wait done {latency:"%s"}`,
							time.Since(start),
						)
					} else {
						log.Warnf(`wait failed {latency:"%s",error:"%s"}`,
							time.Since(start),
							info.Error,
						)
					}
				}
			}
			t.OnPoolTake = func(info trace.PoolTakeStartInfo) func(doneInfo trace.PoolTakeWaitInfo) func(doneInfo trace.PoolTakeDoneInfo) {
				session := info.Session
				log.Tracef(`take start {id:"%s",status:"%s"}`,
					session.ID(),
					session.Status(),
				)
				start := time.Now()
				return func(info trace.PoolTakeWaitInfo) func(info trace.PoolTakeDoneInfo) {
					log.Tracef(`take intermediate {latency:"%s",id:"%s",status:"%s"}`,
						time.Since(start),
						session.ID(),
						session.Status(),
					)
					return func(info trace.PoolTakeDoneInfo) {
						if info.Error == nil {
							log.Tracef(`take done {latency:"%s",id:"%s",status:"%s",took:%t}`,
								time.Since(start),
								session.ID(),
								session.Status(),
								info.Took,
							)
						} else {
							log.Errorf(`take failed {latency:"%s",id:"%s",status:"%s",took:%t,error:"%s"}`,
								time.Since(start),
								session.ID(),
								session.Status(),
								info.Took,
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
