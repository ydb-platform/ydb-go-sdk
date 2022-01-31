// nolint:revive
package ydb_log

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Table makes ydb_trace.Table with internal logging
// nolint:gocyclo
func Table(log Logger, details ydb_trace.Details) ydb_trace.Table {
	log = log.WithName(`table`)
	t := ydb_trace.Table{}
	if details&ydb_trace.TablePoolRetryEvents != 0 {
		// nolint:govet
		log := log.WithName(`retry`)
		t.OnPoolRetry = func(
			info ydb_trace.PoolRetryStartInfo,
		) func(
			info ydb_trace.PoolRetryInternalInfo,
		) func(
			ydb_trace.PoolRetryDoneInfo,
		) {
			idempotent := info.Idempotent
			log.Tracef(`retry start {idempotent:%t}`,
				idempotent,
			)
			start := time.Now()
			return func(info ydb_trace.PoolRetryInternalInfo) func(ydb_trace.PoolRetryDoneInfo) {
				if info.Error == nil {
					log.Tracef(`retry intermediate {latency:"%s",idempotent:%t}`,
						time.Since(start),
						idempotent,
					)
				} else {
					log.Debugf(`retry intermediate {latency:"%s",idempotent:%t,error:"%v"}`,
						time.Since(start),
						idempotent,
						info.Error,
					)
				}
				return func(info ydb_trace.PoolRetryDoneInfo) {
					if info.Error == nil {
						log.Tracef(`retry done {latency:"%s",idempotent:%t,attempts:%d}`,
							time.Since(start),
							idempotent,
							info.Attempts,
						)
					} else {
						log.Errorf(`retry failed {latency:"%s",idempotent:%t,attempts:%d,error:"%v"}`,
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
	// nolint:nestif
	if details&ydb_trace.TableSessionEvents != 0 {
		// nolint:govet
		log := log.WithName(`session`)
		if details&ydb_trace.TableSessionLifeCycleEvents != 0 {
			t.OnSessionNew = func(info ydb_trace.SessionNewStartInfo) func(ydb_trace.SessionNewDoneInfo) {
				log.Tracef(`create start`)
				start := time.Now()
				return func(info ydb_trace.SessionNewDoneInfo) {
					if info.Error == nil {
						log.Tracef(`create done {latency:"%s",id:%d}`,
							time.Since(start),
							info.Session.ID(),
						)
					} else {
						log.Warnf(`create failed {latency:"%s",error:"%v"}`,
							time.Since(start),
							info.Error,
						)
					}
				}
			}
			t.OnSessionDelete = func(info ydb_trace.SessionDeleteStartInfo) func(ydb_trace.SessionDeleteDoneInfo) {
				session := info.Session
				log.Tracef(`delete start {id:"%s",status:"%s"}`,
					session.ID(),
					session.Status(),
				)
				start := time.Now()
				return func(info ydb_trace.SessionDeleteDoneInfo) {
					if info.Error == nil {
						log.Tracef(`delete done {latency:"%s",id:"%s",status:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
						)
					} else {
						log.Warnf(`delete failed {latency:"%s",id:"%s",status:"%s",error:"%v"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							info.Error,
						)
					}
				}
			}
			t.OnSessionKeepAlive = func(info ydb_trace.KeepAliveStartInfo) func(ydb_trace.KeepAliveDoneInfo) {
				session := info.Session
				log.Tracef(`keep-alive start {id:"%s",status:"%s"}`,
					session.ID(),
					session.Status(),
				)
				start := time.Now()
				return func(info ydb_trace.KeepAliveDoneInfo) {
					if info.Error == nil {
						log.Tracef(`keep-alive done {latency:"%s",id:"%s",status:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
						)
					} else {
						log.Warnf(`keep-alive failed {latency:"%s",id:"%s",status:"%s",error:"%v"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							info.Error,
						)
					}
				}
			}
		}
		if details&ydb_trace.TableSessionQueryEvents != 0 {
			// nolint:govet
			log := log.WithName(`query`)
			if details&ydb_trace.TableSessionQueryInvokeEvents != 0 {
				// nolint:govet
				log := log.WithName(`invoke`)
				t.OnSessionQueryPrepare = func(
					info ydb_trace.SessionQueryPrepareStartInfo,
				) func(
					ydb_trace.PrepareDataQueryDoneInfo,
				) {
					session := info.Session
					query := info.Query
					log.Tracef(`prepare start {id:"%s",status:"%s",query:"%s"}`,
						session.ID(),
						session.Status(),
						query,
					)
					start := time.Now()
					return func(info ydb_trace.PrepareDataQueryDoneInfo) {
						if info.Error == nil {
							log.Debugf(`prepare done {latency:"%s",id:"%s",status:"%s",query:"%s",result:"%s"}`,
								time.Since(start),
								session.ID(),
								session.Status(),
								query,
								info.Result,
							)
						} else {
							log.Errorf(`prepare failed {latency:"%s",id:"%s",status:"%s",query:"%s",error:"%v"}`,
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
					info ydb_trace.ExecuteDataQueryStartInfo,
				) func(
					ydb_trace.SessionQueryPrepareDoneInfo,
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
					return func(info ydb_trace.SessionQueryPrepareDoneInfo) {
						if info.Error == nil {
							tx := info.Tx
							log.Debugf(
								`execute done {latency:"%s",id:"%s",status:"%s",tx:"%s",query:"%s",params:"%s",prepared:%t,result:{err:"%v"}}`,
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
							log.Errorf(`execute failed {latency:"%s",id:"%s",status:"%s",query:"%s",params:"%s",prepared:%t,error:"%v"}`,
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
			if details&ydb_trace.TableSessionQueryStreamEvents != 0 {
				// nolint:govet
				log := log.WithName(`stream`)
				t.OnSessionQueryStreamExecute = func(
					info ydb_trace.SessionQueryStreamExecuteStartInfo,
				) func(
					ydb_trace.SessionQueryStreamExecuteDoneInfo,
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
					return func(info ydb_trace.SessionQueryStreamExecuteDoneInfo) {
						if info.Error == nil {
							log.Debugf(`execute done {latency:"%s",id:"%s",status:"%s",query:"%s",params:"%s"}`,
								time.Since(start),
								session.ID(),
								session.Status(),
								query,
								params,
							)
						} else {
							log.Errorf(`execute failed {latency:"%s",id:"%s",status:"%s",query:"%s",params:"%s",error:"%v"}`,
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
				t.OnSessionQueryStreamRead = func(
					info ydb_trace.SessionQueryStreamReadStartInfo,
				) func(
					ydb_trace.SessionQueryStreamReadDoneInfo,
				) {
					session := info.Session
					log.Tracef(`read start {id:"%s",status:"%s"}`,
						session.ID(),
						session.Status(),
					)
					start := time.Now()
					return func(info ydb_trace.SessionQueryStreamReadDoneInfo) {
						if info.Error == nil {
							log.Debugf(`read done {latency:"%s",id:"%s",status:"%s"}`,
								time.Since(start),
								session.ID(),
								session.Status(),
							)
						} else {
							log.Errorf(`read failed {latency:"%s",id:"%s",status:"%s",error:"%v"}`,
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
		if details&ydb_trace.TableSessionTransactionEvents != 0 {
			// nolint:govet
			log := log.WithName(`transaction`)
			t.OnSessionTransactionBegin = func(
				info ydb_trace.SessionTransactionBeginStartInfo,
			) func(
				ydb_trace.SessionTransactionBeginDoneInfo,
			) {
				session := info.Session
				log.Tracef(`begin start {id:"%s",status:"%s"}`,
					session.ID(),
					session.Status(),
				)
				start := time.Now()
				return func(info ydb_trace.SessionTransactionBeginDoneInfo) {
					if info.Error == nil {
						log.Debugf(`begin done {latency:"%s",id:"%s",status:"%s",tx:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							info.Tx.ID(),
						)
					} else {
						log.Warnf(`begin failed {latency:"%s",id:"%s",status:"%s",error:"%v"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							info.Error,
						)
					}
				}
			}
			t.OnSessionTransactionCommit = func(
				info ydb_trace.SessionTransactionCommitStartInfo,
			) func(
				ydb_trace.SessionTransactionCommitDoneInfo,
			) {
				session := info.Session
				tx := info.Tx
				log.Tracef(`commit start {id:"%s",status:"%s",tx:"%s"}`,
					session.ID(),
					session.Status(),
					tx.ID(),
				)
				start := time.Now()
				return func(info ydb_trace.SessionTransactionCommitDoneInfo) {
					if info.Error == nil {
						log.Debugf(`commit done {latency:"%s",id:"%s",status:"%s",tx:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							tx.ID(),
						)
					} else {
						log.Errorf(`commit failed {latency:"%s",id:"%s",status:"%s",tx:"%s",error:"%v"}`,
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
				info ydb_trace.SessionTransactionRollbackStartInfo,
			) func(
				ydb_trace.SessionTransactionRollbackDoneInfo,
			) {
				session := info.Session
				tx := info.Tx
				log.Tracef(`rollback start {id:"%s",status:"%s",tx:"%s"}`,
					session.ID(),
					session.Status(),
					tx.ID(),
				)
				start := time.Now()
				return func(info ydb_trace.SessionTransactionRollbackDoneInfo) {
					if info.Error == nil {
						log.Debugf(`rollback done {latency:"%s",id:"%s",status:"%s",tx:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							tx.ID(),
						)
					} else {
						log.Errorf(`rollback failed {latency:"%s",id:"%s",status:"%s",tx:"%s",error:"%v"}`,
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
	if details&ydb_trace.TablePoolEvents != 0 {
		// nolint:govet
		log := log.WithName(`pool`)
		if details&ydb_trace.TablePoolLifeCycleEvents != 0 {
			t.OnPoolInit = func(info ydb_trace.PoolInitStartInfo) func(ydb_trace.PoolInitDoneInfo) {
				log.Infof(`initialize start`)
				start := time.Now()
				return func(info ydb_trace.PoolInitDoneInfo) {
					log.Infof(`initialize done {latency:"%s",size:{min:%d,max:%d}}`,
						time.Since(start),
						info.KeepAliveMinSize,
						info.Limit,
					)
				}
			}
			t.OnPoolClose = func(info ydb_trace.PoolCloseStartInfo) func(ydb_trace.PoolCloseDoneInfo) {
				log.Infof(`close start`)
				start := time.Now()
				return func(info ydb_trace.PoolCloseDoneInfo) {
					if info.Error == nil {
						log.Infof(`close done {latency:"%s"}`,
							time.Since(start),
						)
					} else {
						log.Errorf(`close failed {latency:"%s",error:"%v"}`,
							time.Since(start),
							info.Error,
						)
					}
				}
			}
		}
		if details&ydb_trace.TablePoolSessionLifeCycleEvents != 0 {
			// nolint:govet
			log := log.WithName(`session`)
			t.OnPoolSessionNew = func(info ydb_trace.PoolSessionNewStartInfo) func(ydb_trace.PoolSessionNewDoneInfo) {
				log.Tracef(`create start`)
				start := time.Now()
				return func(info ydb_trace.PoolSessionNewDoneInfo) {
					if info.Error == nil {
						session := info.Session
						log.Debugf(`create done {latency:"%s",id:"%s",status:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
						)
					} else {
						log.Errorf(`create failed {latency:"%s",error:"%v"}`,
							time.Since(start),
							info.Error,
						)
					}
				}
			}
			t.OnPoolSessionClose = func(info ydb_trace.PoolSessionCloseStartInfo) func(ydb_trace.PoolSessionCloseDoneInfo) {
				session := info.Session
				log.Tracef(`close start {id:"%s",status:"%s"}`,
					session.ID(),
					session.Status(),
				)
				start := time.Now()
				return func(info ydb_trace.PoolSessionCloseDoneInfo) {
					log.Debugf(`close done {latency:"%s",id:"%s",status:"%s"}`,
						time.Since(start),
						session.ID(),
						session.Status(),
					)
				}
			}
		}
		if details&ydb_trace.TablePoolAPIEvents != 0 {
			t.OnPoolPut = func(info ydb_trace.PoolPutStartInfo) func(ydb_trace.PoolPutDoneInfo) {
				session := info.Session
				log.Tracef(`put start {id:"%s",status:"%s"}`,
					session.ID(),
					session.Status(),
				)
				start := time.Now()
				return func(info ydb_trace.PoolPutDoneInfo) {
					if info.Error == nil {
						log.Tracef(`put done {latency:"%s",id:"%s",status:"%s"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
						)
					} else {
						log.Errorf(`put failed {latency:"%s",id:"%s",status:"%s",error:"%v"}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							info.Error,
						)
					}
				}
			}
			t.OnPoolGet = func(info ydb_trace.PoolGetStartInfo) func(ydb_trace.PoolGetDoneInfo) {
				log.Tracef(`get start`)
				start := time.Now()
				return func(info ydb_trace.PoolGetDoneInfo) {
					if info.Error == nil {
						session := info.Session
						log.Tracef(`get done {latency:"%s",id:"%s",status:"%s",attempts:%d}`,
							time.Since(start),
							session.ID(),
							session.Status(),
							info.Attempts,
						)
					} else {
						log.Warnf(`get failed {latency:"%s",attempts:%d,error:"%v"}`,
							time.Since(start),
							info.Attempts,
							info.Error,
						)
					}
				}
			}
			t.OnPoolWait = func(info ydb_trace.PoolWaitStartInfo) func(ydb_trace.PoolWaitDoneInfo) {
				log.Tracef(`wait start`)
				start := time.Now()
				return func(info ydb_trace.PoolWaitDoneInfo) {
					if info.Error == nil {
						if info.Session == nil {
							log.Tracef(`wait done without any significant result {latency:"%s"}`,
								time.Since(start),
							)
						} else {
							log.Tracef(`wait done {latency:"%s",id:"%s",status:"%s"}`,
								time.Since(start),
								info.Session.ID(),
								info.Session.Status(),
							)
						}
					} else {
						if info.Session == nil {
							log.Tracef(`wait failed {latency:"%s",error:"%v"}`,
								time.Since(start),
								info.Error,
							)
						} else {
							log.Tracef(`wait failed with session {latency:"%s",id:"%s",status:"%s",error:"%v"}`,
								time.Since(start),
								info.Session.ID(),
								info.Session.Status(),
								info.Error,
							)
						}
					}
				}
			}
			t.OnPoolTake = func(
				info ydb_trace.PoolTakeStartInfo,
			) func(
				doneInfo ydb_trace.PoolTakeWaitInfo,
			) func(
				doneInfo ydb_trace.PoolTakeDoneInfo,
			) {
				session := info.Session
				log.Tracef(`take start {id:"%s",status:"%s"}`,
					session.ID(),
					session.Status(),
				)
				start := time.Now()
				return func(info ydb_trace.PoolTakeWaitInfo) func(info ydb_trace.PoolTakeDoneInfo) {
					log.Tracef(`take intermediate {latency:"%s",id:"%s",status:"%s"}`,
						time.Since(start),
						session.ID(),
						session.Status(),
					)
					return func(info ydb_trace.PoolTakeDoneInfo) {
						if info.Error == nil {
							log.Tracef(`take done {latency:"%s",id:"%s",status:"%s",took:%t}`,
								time.Since(start),
								session.ID(),
								session.Status(),
								info.Took,
							)
						} else {
							log.Errorf(`take failed {latency:"%s",id:"%s",status:"%s",took:%t,error:"%v"}`,
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
