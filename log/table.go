package log

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Table makes trace.Table with logging events from details
func Table(l Logger, d trace.Detailer, opts ...Option) (t trace.Table) {
	return internalTable(wrapLogger(l, opts...), d)
}

func internalTable(l *wrapper, d trace.Detailer) (t trace.Table) {
	t.OnDo = onDo(l, d)
	t.OnDoTx = onDoTx(l, d)
	t.OnCreateSession = onCreateSession(l, d)
	t.OnSessionNew = onSessionNew(l, d)
	t.OnSessionDelete = onSessionDelete(l, d)
	t.OnSessionKeepAlive = onSessionKeepAlive(l, d)
	t.OnSessionQueryPrepare = onSessionQueryPrepare(l, d)
	t.OnSessionQueryExecute = onSessionQueryExecute(l, d)
	t.OnSessionQueryStreamExecute = onSessionQueryStreamExecute(l, d)
	t.OnSessionQueryStreamRead = onSessionQueryStreamRead(l, d)
	t.OnSessionTransactionBegin = onSessionTransactionBegin(l, d)
	t.OnSessionTransactionCommit = onSessionTransactionCommit(l, d)
	t.OnSessionTransactionRollback = OnSessionTransactionRollback(l, d)
	t.OnInit = onInitInternalTable(l, d)
	t.OnClose = onCloseInternalTable(l, d)
	t.OnPoolStateChange = onPoolStateChange(l, d)
	t.OnPoolSessionAdd = onPoolSessionAdd(l, d)
	t.OnPoolSessionRemove = onPoolSessionRemove(l, d)
	t.OnPoolPut = onPoolPut(l, d)
	t.OnPoolGet = onPoolGet(l, d)
	t.OnPoolWait = onPoolWait(l, d)

	return t
}

func onDo(
	l *wrapper,
	d trace.Detailer,
) func(info trace.TableDoStartInfo) func(info trace.TableDoIntermediateInfo) func(trace.TableDoDoneInfo) {
	return func(
		info trace.TableDoStartInfo,
	) func(
		info trace.TableDoIntermediateInfo,
	) func(
		trace.TableDoDoneInfo,
	) {
		if d.Details()&trace.TablePoolAPIEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "table", "do")
		idempotent := info.Idempotent
		label := info.Label
		l.Log(ctx, "start",
			Bool("idempotent", idempotent),
			String("label", label),
		)
		start := time.Now()

		return func(info trace.TableDoIntermediateInfo) func(trace.TableDoDoneInfo) {
			if info.Error == nil {
				l.Log(ctx, "done",
					latencyField(start),
					Bool("idempotent", idempotent),
					String("label", label),
				)
			} else {
				lvl := WARN
				if !xerrors.IsYdb(info.Error) {
					lvl = DEBUG
				}
				m := retry.Check(info.Error)
				l.Log(WithLevel(ctx, lvl), "failed",
					latencyField(start),
					Bool("idempotent", idempotent),
					String("label", label),
					Error(info.Error),
					Bool("retryable", m.MustRetry(idempotent)),
					Int64("code", m.StatusCode()),
					Bool("deleteSession", m.MustDeleteSession()),
					versionField(),
				)
			}

			return func(info trace.TableDoDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						latencyField(start),
						Bool("idempotent", idempotent),
						String("label", label),
						Int("attempts", info.Attempts),
					)
				} else {
					lvl := ERROR
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					m := retry.Check(info.Error)
					l.Log(WithLevel(ctx, lvl), "done",
						latencyField(start),
						Bool("idempotent", idempotent),
						String("label", label),
						Int("attempts", info.Attempts),
						Error(info.Error),
						Bool("retryable", m.MustRetry(idempotent)),
						Int64("code", m.StatusCode()),
						Bool("deleteSession", m.MustDeleteSession()),
						versionField(),
					)
				}
			}
		}
	}
}

func onDoTx(
	l *wrapper,
	d trace.Detailer,
) func(info trace.TableDoTxStartInfo) func(info trace.TableDoTxIntermediateInfo) func(trace.TableDoTxDoneInfo) {
	return func(
		info trace.TableDoTxStartInfo,
	) func(
		info trace.TableDoTxIntermediateInfo,
	) func(
		trace.TableDoTxDoneInfo,
	) {
		if d.Details()&trace.TablePoolAPIEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "table", "do", "tx")
		idempotent := info.Idempotent
		label := info.Label
		l.Log(ctx, "start",
			Bool("idempotent", idempotent),
			String("label", label),
		)
		start := time.Now()

		return func(info trace.TableDoTxIntermediateInfo) func(trace.TableDoTxDoneInfo) {
			if info.Error == nil {
				l.Log(ctx, "done",
					latencyField(start),
					Bool("idempotent", idempotent),
					String("label", label),
				)
			} else {
				lvl := ERROR
				if !xerrors.IsYdb(info.Error) {
					lvl = DEBUG
				}
				m := retry.Check(info.Error)
				l.Log(WithLevel(ctx, lvl), "done",
					latencyField(start),
					Bool("idempotent", idempotent),
					String("label", label),
					Error(info.Error),
					Bool("retryable", m.MustRetry(idempotent)),
					Int64("code", m.StatusCode()),
					Bool("deleteSession", m.MustDeleteSession()),
					versionField(),
				)
			}

			return func(info trace.TableDoTxDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						latencyField(start),
						Bool("idempotent", idempotent),
						String("label", label),
						Int("attempts", info.Attempts),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					m := retry.Check(info.Error)
					l.Log(WithLevel(ctx, lvl), "done",
						latencyField(start),
						Bool("idempotent", idempotent),
						String("label", label),
						Int("attempts", info.Attempts),
						Error(info.Error),
						Bool("retryable", m.MustRetry(idempotent)),
						Int64("code", m.StatusCode()),
						Bool("deleteSession", m.MustDeleteSession()),
						versionField(),
					)
				}
			}
		}
	}
}

func onCreateSession(
	l *wrapper,
	d trace.Detailer,
) func(
	info trace.TableCreateSessionStartInfo) func(
	info trace.TableCreateSessionIntermediateInfo) func(trace.TableCreateSessionDoneInfo) {
	return func(
		info trace.TableCreateSessionStartInfo,
	) func(
		info trace.TableCreateSessionIntermediateInfo,
	) func(
		trace.TableCreateSessionDoneInfo,
	) {
		if d.Details()&trace.TablePoolAPIEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "table", "create", "session")
		l.Log(ctx, "start")
		start := time.Now()

		return func(info trace.TableCreateSessionIntermediateInfo) func(trace.TableCreateSessionDoneInfo) {
			if info.Error == nil {
				l.Log(ctx, "intermediate",
					latencyField(start),
				)
			} else {
				l.Log(WithLevel(ctx, ERROR), "intermediate",
					latencyField(start),
					Error(info.Error),
					versionField(),
				)
			}

			return func(info trace.TableCreateSessionDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						latencyField(start),
						Int("attempts", info.Attempts),
						String("session_id", info.Session.ID()),
						String("session_status", info.Session.Status()),
					)
				} else {
					l.Log(WithLevel(ctx, ERROR), "failed",
						latencyField(start),
						Int("attempts", info.Attempts),
						Error(info.Error),
						versionField(),
					)
				}
			}
		}
	}
}

func onSessionNew(
	l *wrapper,
	d trace.Detailer,
) func(info trace.TableSessionNewStartInfo) func(trace.TableSessionNewDoneInfo) {
	return func(info trace.TableSessionNewStartInfo) func(trace.TableSessionNewDoneInfo) {
		if d.Details()&trace.TableSessionEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "table", "session", "new")
		l.Log(ctx, "start")
		start := time.Now()

		return func(info trace.TableSessionNewDoneInfo) {
			if info.Error == nil {
				if info.Session != nil {
					l.Log(ctx, "done",
						latencyField(start),
						String("id", info.Session.ID()),
					)
				} else {
					l.Log(WithLevel(ctx, WARN), "failed",
						latencyField(start),
						versionField(),
					)
				}
			} else {
				l.Log(WithLevel(ctx, WARN), "failed",
					latencyField(start),
					Error(info.Error),
					versionField(),
				)
			}
		}
	}
}

func onSessionDelete(
	l *wrapper,
	d trace.Detailer,
) func(info trace.TableSessionDeleteStartInfo) func(trace.TableSessionDeleteDoneInfo) {
	return func(info trace.TableSessionDeleteStartInfo) func(trace.TableSessionDeleteDoneInfo) {
		if d.Details()&trace.TableSessionEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "table", "session", "delete")
		session := info.Session
		l.Log(ctx, "start",
			String("id", info.Session.ID()),
			String("status", info.Session.Status()),
		)
		start := time.Now()

		return func(info trace.TableSessionDeleteDoneInfo) {
			if info.Error == nil {
				l.Log(ctx, "done",
					latencyField(start),
					String("id", session.ID()),
					String("status", session.Status()),
				)
			} else {
				l.Log(WithLevel(ctx, WARN), "failed",
					latencyField(start),
					String("id", session.ID()),
					String("status", session.Status()),
					Error(info.Error),
					versionField(),
				)
			}
		}
	}
}

func onSessionKeepAlive(
	l *wrapper,
	d trace.Detailer,
) func(info trace.TableKeepAliveStartInfo) func(trace.TableKeepAliveDoneInfo) {
	return func(info trace.TableKeepAliveStartInfo) func(trace.TableKeepAliveDoneInfo) {
		if d.Details()&trace.TableSessionEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "table", "session", "keep", "alive")
		session := info.Session
		l.Log(ctx, "start",
			String("id", session.ID()),
			String("status", session.Status()),
		)
		start := time.Now()

		return func(info trace.TableKeepAliveDoneInfo) {
			if info.Error == nil {
				l.Log(ctx, "done",
					latencyField(start),
					String("id", session.ID()),
					String("status", session.Status()),
				)
			} else {
				l.Log(WithLevel(ctx, WARN), "failed",
					latencyField(start),
					String("id", session.ID()),
					String("status", session.Status()),
					Error(info.Error),
					versionField(),
				)
			}
		}
	}
}

func onSessionQueryPrepare(
	l *wrapper,
	d trace.Detailer,
) func(info trace.TablePrepareDataQueryStartInfo) func(trace.TablePrepareDataQueryDoneInfo) {
	return func(
		info trace.TablePrepareDataQueryStartInfo,
	) func(
		trace.TablePrepareDataQueryDoneInfo,
	) {
		if d.Details()&trace.TableSessionQueryInvokeEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "table", "session", "query", "prepare")
		session := info.Session
		query := info.Query
		l.Log(ctx, "start",
			appendFieldByCondition(l.logQuery,
				String("query", info.Query),
				String("id", session.ID()),
				String("status", session.Status()),
			)...,
		)
		start := time.Now()

		return func(info trace.TablePrepareDataQueryDoneInfo) {
			if info.Error == nil {
				l.Log(ctx, "done",
					appendFieldByCondition(l.logQuery,
						Stringer("result", info.Result),
						appendFieldByCondition(l.logQuery,
							String("query", query),
							String("id", session.ID()),
							String("status", session.Status()),
							latencyField(start),
						)...,
					)...,
				)
			} else {
				l.Log(WithLevel(ctx, ERROR), "failed",
					appendFieldByCondition(l.logQuery,
						String("query", query),
						Error(info.Error),
						String("id", session.ID()),
						String("status", session.Status()),
						latencyField(start),
						versionField(),
					)...,
				)
			}
		}
	}
}

func onSessionQueryExecute(
	l *wrapper,
	d trace.Detailer,
) func(info trace.TableExecuteDataQueryStartInfo) func(trace.TableExecuteDataQueryDoneInfo) {
	return func(
		info trace.TableExecuteDataQueryStartInfo,
	) func(
		trace.TableExecuteDataQueryDoneInfo,
	) {
		if d.Details()&trace.TableSessionQueryInvokeEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "table", "session", "query", "execute")
		session := info.Session
		query := info.Query
		l.Log(ctx, "start",
			appendFieldByCondition(l.logQuery,
				Stringer("query", info.Query),
				String("id", session.ID()),
				String("status", session.Status()),
			)...,
		)
		start := time.Now()

		return func(info trace.TableExecuteDataQueryDoneInfo) {
			if info.Error == nil {
				tx := info.Tx
				l.Log(ctx, "done",
					appendFieldByCondition(l.logQuery,
						Stringer("query", query),
						String("id", session.ID()),
						String("tx", tx.ID()),
						String("status", session.Status()),
						Bool("prepared", info.Prepared),
						NamedError("result_err", info.Result.Err()),
						latencyField(start),
					)...,
				)
			} else {
				l.Log(WithLevel(ctx, ERROR), "failed",
					appendFieldByCondition(l.logQuery,
						Stringer("query", query),
						Error(info.Error),
						String("id", session.ID()),
						String("status", session.Status()),
						Bool("prepared", info.Prepared),
						latencyField(start),
						versionField(),
					)...,
				)
			}
		}
	}
}

func onSessionQueryStreamExecute(
	l *wrapper,
	d trace.Detailer,
) func(
	info trace.TableSessionQueryStreamExecuteStartInfo) func(
	trace.TableSessionQueryStreamExecuteIntermediateInfo) func(trace.TableSessionQueryStreamExecuteDoneInfo) {
	return func(
		info trace.TableSessionQueryStreamExecuteStartInfo,
	) func(
		trace.TableSessionQueryStreamExecuteIntermediateInfo,
	) func(
		trace.TableSessionQueryStreamExecuteDoneInfo,
	) {
		if d.Details()&trace.TableSessionQueryStreamEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "table", "session", "query", "stream", "execute")
		session := info.Session
		query := info.Query
		l.Log(ctx, "start",
			appendFieldByCondition(l.logQuery,
				Stringer("query", info.Query),
				String("id", session.ID()),
				String("status", session.Status()),
			)...,
		)
		start := time.Now()

		return func(
			info trace.TableSessionQueryStreamExecuteIntermediateInfo,
		) func(
			trace.TableSessionQueryStreamExecuteDoneInfo,
		) {
			if info.Error == nil {
				l.Log(ctx, "intermediate")
			} else {
				l.Log(WithLevel(ctx, WARN), "failed",
					Error(info.Error),
					versionField(),
				)
			}

			return func(info trace.TableSessionQueryStreamExecuteDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						appendFieldByCondition(l.logQuery,
							Stringer("query", query),
							Error(info.Error),
							String("id", session.ID()),
							String("status", session.Status()),
							latencyField(start),
						)...,
					)
				} else {
					l.Log(WithLevel(ctx, ERROR), "failed",
						appendFieldByCondition(l.logQuery,
							Stringer("query", query),
							Error(info.Error),
							String("id", session.ID()),
							String("status", session.Status()),
							latencyField(start),
							versionField(),
						)...,
					)
				}
			}
		}
	}
}

func onSessionQueryStreamRead(
	l *wrapper,
	d trace.Detailer,
) func(
	info trace.TableSessionQueryStreamReadStartInfo) func(
	intermediateInfo trace.TableSessionQueryStreamReadIntermediateInfo) func(
	trace.TableSessionQueryStreamReadDoneInfo) {
	return func(
		info trace.TableSessionQueryStreamReadStartInfo,
	) func(
		intermediateInfo trace.TableSessionQueryStreamReadIntermediateInfo,
	) func(
		trace.TableSessionQueryStreamReadDoneInfo,
	) {
		if d.Details()&trace.TableSessionQueryStreamEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "table", "session", "query", "stream", "read")
		session := info.Session
		l.Log(ctx, "start",
			String("id", session.ID()),
			String("status", session.Status()),
		)
		start := time.Now()

		return func(
			info trace.TableSessionQueryStreamReadIntermediateInfo,
		) func(
			trace.TableSessionQueryStreamReadDoneInfo,
		) {
			if info.Error == nil {
				l.Log(ctx, "intermediate")
			} else {
				l.Log(WithLevel(ctx, WARN), "failed",
					Error(info.Error),
					versionField(),
				)
			}

			return func(info trace.TableSessionQueryStreamReadDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						latencyField(start),
						String("id", session.ID()),
						String("status", session.Status()),
					)
				} else {
					l.Log(WithLevel(ctx, ERROR), "failed",
						latencyField(start),
						String("id", session.ID()),
						String("status", session.Status()),
						Error(info.Error),
						versionField(),
					)
				}
			}
		}
	}
}

func onSessionTransactionBegin(
	l *wrapper,
	d trace.Detailer,
) func(info trace.TableSessionTransactionBeginStartInfo) func(trace.TableSessionTransactionBeginDoneInfo) {
	return func(
		info trace.TableSessionTransactionBeginStartInfo,
	) func(
		trace.TableSessionTransactionBeginDoneInfo,
	) {
		if d.Details()&trace.TableSessionTransactionEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "table", "session", "tx", "begin")
		session := info.Session
		l.Log(ctx, "start",
			String("id", session.ID()),
			String("status", session.Status()),
		)
		start := time.Now()

		return func(info trace.TableSessionTransactionBeginDoneInfo) {
			if info.Error == nil {
				l.Log(ctx, "done",
					latencyField(start),
					String("id", session.ID()),
					String("status", session.Status()),
					String("tx", info.Tx.ID()),
				)
			} else {
				l.Log(WithLevel(ctx, WARN), "failed",
					latencyField(start),
					String("id", session.ID()),
					String("status", session.Status()),
					Error(info.Error),
					versionField(),
				)
			}
		}
	}
}

func onSessionTransactionCommit(
	l *wrapper,
	d trace.Detailer,
) func(info trace.TableSessionTransactionCommitStartInfo) func(trace.TableSessionTransactionCommitDoneInfo) {
	return func(
		info trace.TableSessionTransactionCommitStartInfo,
	) func(
		trace.TableSessionTransactionCommitDoneInfo,
	) {
		if d.Details()&trace.TableSessionTransactionEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "table", "session", "tx", "commit")
		session := info.Session
		tx := info.Tx
		l.Log(ctx, "start",
			String("id", session.ID()),
			String("status", session.Status()),
			String("tx", info.Tx.ID()),
		)
		start := time.Now()

		return func(info trace.TableSessionTransactionCommitDoneInfo) {
			if info.Error == nil {
				l.Log(ctx, "done",
					latencyField(start),
					String("id", session.ID()),
					String("status", session.Status()),
					String("tx", tx.ID()),
				)
			} else {
				l.Log(WithLevel(ctx, ERROR), "failed",
					latencyField(start),
					String("id", session.ID()),
					String("status", session.Status()),
					String("tx", tx.ID()),
					Error(info.Error),
					versionField(),
				)
			}
		}
	}
}

func OnSessionTransactionRollback(
	l *wrapper,
	d trace.Detailer,
) func(info trace.TableSessionTransactionRollbackStartInfo) func(trace.TableSessionTransactionRollbackDoneInfo) {
	return func(
		info trace.TableSessionTransactionRollbackStartInfo,
	) func(
		trace.TableSessionTransactionRollbackDoneInfo,
	) {
		if d.Details()&trace.TableSessionTransactionEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "table", "session", "tx", "rollback")
		session := info.Session
		tx := info.Tx
		l.Log(ctx, "start",
			String("id", session.ID()),
			String("status", session.Status()),
			String("tx", tx.ID()),
		)
		start := time.Now()

		return func(info trace.TableSessionTransactionRollbackDoneInfo) {
			if info.Error == nil {
				l.Log(ctx, "done",
					latencyField(start),
					String("id", session.ID()),
					String("status", session.Status()),
					String("tx", tx.ID()),
				)
			} else {
				l.Log(WithLevel(ctx, ERROR), "failed",
					latencyField(start),
					String("id", session.ID()),
					String("status", session.Status()),
					String("tx", tx.ID()),
					Error(info.Error),
					versionField(),
				)
			}
		}
	}
}

func onInitInternalTable(
	l *wrapper,
	d trace.Detailer,
) func(info trace.TableInitStartInfo) func(trace.TableInitDoneInfo) {
	return func(info trace.TableInitStartInfo) func(trace.TableInitDoneInfo) {
		if d.Details()&trace.TableEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "table", "init")
		l.Log(ctx, "start")
		start := time.Now()

		return func(info trace.TableInitDoneInfo) {
			l.Log(WithLevel(ctx, INFO), "done",
				latencyField(start),
				Int("size_max", info.Limit),
			)
		}
	}
}

func onCloseInternalTable(
	l *wrapper,
	d trace.Detailer,
) func(info trace.TableCloseStartInfo) func(trace.TableCloseDoneInfo) {
	return func(info trace.TableCloseStartInfo) func(trace.TableCloseDoneInfo) {
		if d.Details()&trace.TableEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "table", "close")
		l.Log(ctx, "start")
		start := time.Now()

		return func(info trace.TableCloseDoneInfo) {
			if info.Error == nil {
				l.Log(WithLevel(ctx, INFO), "done",
					latencyField(start),
				)
			} else {
				l.Log(WithLevel(ctx, ERROR), "failed",
					latencyField(start),
					Error(info.Error),
					versionField(),
				)
			}
		}
	}
}

func onPoolStateChange(
	l *wrapper,
	d trace.Detailer,
) func(info trace.TablePoolStateChangeInfo) {
	return func(info trace.TablePoolStateChangeInfo) {
		if d.Details()&trace.TablePoolLifeCycleEvents == 0 {
			return
		}
		ctx := with(context.Background(), TRACE, "ydb", "table", "pool", "state", "change")
		l.Log(WithLevel(ctx, DEBUG), "",
			Int("size", info.Size),
			String("event", info.Event),
		)
	}
}

func onPoolSessionAdd(
	l *wrapper,
	d trace.Detailer,
) func(info trace.TablePoolSessionAddInfo) {
	return func(info trace.TablePoolSessionAddInfo) {
		if d.Details()&trace.TablePoolLifeCycleEvents == 0 {
			return
		}
		ctx := with(context.Background(), TRACE, "ydb", "table", "pool", "session", "add")
		l.Log(ctx, "start",
			String("id", info.Session.ID()),
			String("status", info.Session.Status()),
		)
	}
}

func onPoolSessionRemove(
	l *wrapper,
	d trace.Detailer,
) func(info trace.TablePoolSessionRemoveInfo) {
	return func(info trace.TablePoolSessionRemoveInfo) {
		if d.Details()&trace.TablePoolLifeCycleEvents == 0 {
			return
		}
		ctx := with(context.Background(), TRACE, "ydb", "table", "pool", "session", "remove")
		l.Log(ctx, "start",
			String("id", info.Session.ID()),
			String("status", info.Session.Status()),
		)
	}
}

func onPoolPut(
	l *wrapper,
	d trace.Detailer,
) func(info trace.TablePoolPutStartInfo) func(trace.TablePoolPutDoneInfo) {
	return func(info trace.TablePoolPutStartInfo) func(trace.TablePoolPutDoneInfo) {
		if d.Details()&trace.TablePoolAPIEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "table", "pool", "put")
		session := info.Session
		l.Log(ctx, "start",
			String("id", session.ID()),
			String("status", session.Status()),
		)
		start := time.Now()

		return func(info trace.TablePoolPutDoneInfo) {
			if info.Error == nil {
				l.Log(ctx, "done",
					latencyField(start),
					String("id", session.ID()),
					String("status", session.Status()),
				)
			} else {
				l.Log(WithLevel(ctx, ERROR), "failed",
					latencyField(start),
					String("id", session.ID()),
					String("status", session.Status()),
					Error(info.Error),
					versionField(),
				)
			}
		}
	}
}

func onPoolGet(
	l *wrapper,
	d trace.Detailer,
) func(info trace.TablePoolGetStartInfo) func(trace.TablePoolGetDoneInfo) {
	return func(info trace.TablePoolGetStartInfo) func(trace.TablePoolGetDoneInfo) {
		if d.Details()&trace.TablePoolAPIEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "table", "pool", "get")
		l.Log(ctx, "start")
		start := time.Now()

		return func(info trace.TablePoolGetDoneInfo) {
			if info.Error == nil {
				session := info.Session
				l.Log(ctx, "done",
					latencyField(start),
					String("id", session.ID()),
					String("status", session.Status()),
					Int("attempts", info.Attempts),
				)
			} else {
				l.Log(WithLevel(ctx, WARN), "failed",
					latencyField(start),
					Int("attempts", info.Attempts),
					Error(info.Error),
					versionField(),
				)
			}
		}
	}
}

func onPoolWait(
	l *wrapper,
	d trace.Detailer,
) func(info trace.TablePoolWaitStartInfo) func(trace.TablePoolWaitDoneInfo) {
	return func(info trace.TablePoolWaitStartInfo) func(trace.TablePoolWaitDoneInfo) {
		if d.Details()&trace.TablePoolAPIEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "table", "pool", "wait")
		l.Log(ctx, "start")
		start := time.Now()

		return func(info trace.TablePoolWaitDoneInfo) {
			fields := []Field{
				latencyField(start),
			}
			if info.Session != nil {
				fields = append(fields,
					String("id", info.Session.ID()),
					String("status", info.Session.Status()),
				)
			}
			if info.Error == nil {
				l.Log(ctx, "done", fields...)
			} else {
				fields = append(fields, Error(info.Error))
				l.Log(WithLevel(ctx, WARN), "failed", fields...)
			}
		}
	}
}
