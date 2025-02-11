package log

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/kv"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Query makes trace.Query with logging events from details
func Query(l Logger, d trace.Detailer, opts ...Option) (t trace.Query) {
	return internalQuery(wrapLogger(l, opts...), d)
}

//nolint:gocyclo,funlen
func internalQuery(
	l *wrapper, //nolint:interfacer
	d trace.Detailer,
) trace.Query {
	return trace.Query{
		OnNew: func(info trace.QueryNewStartInfo) func(info trace.QueryNewDoneInfo) {
			if d.Details()&trace.QueryEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "new")
			l.Log(ctx, "starting create new query client")
			start := time.Now()

			return func(info trace.QueryNewDoneInfo) {
				l.Log(WithLevel(ctx, INFO), "create query client done",
					kv.Latency(start),
				)
			}
		},
		OnClose: func(info trace.QueryCloseStartInfo) func(info trace.QueryCloseDoneInfo) {
			if d.Details()&trace.QueryEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "close")
			l.Log(ctx, "query client starting close...")
			start := time.Now()

			return func(info trace.QueryCloseDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "query close done",
						kv.Latency(start),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "query close failed",
						kv.Latency(start),
						kv.Error(info.Error),
						kv.Version(),
					)
				}
			}
		},
		OnPoolNew: func(info trace.QueryPoolNewStartInfo) func(trace.QueryPoolNewDoneInfo) {
			if d.Details()&trace.QueryPoolEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "pool", "new")
			l.Log(ctx, "query service starting create pool...")
			start := time.Now()

			return func(info trace.QueryPoolNewDoneInfo) {
				l.Log(WithLevel(ctx, INFO), "query service create pool done",
					kv.Latency(start),
					kv.Int("Limit", info.Limit),
				)
			}
		},
		OnPoolClose: func(info trace.QueryPoolCloseStartInfo) func(trace.QueryPoolCloseDoneInfo) {
			if d.Details()&trace.QueryPoolEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "pool", "close")
			l.Log(ctx, "query service starting close pool...")
			start := time.Now()

			return func(info trace.QueryPoolCloseDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "query service close done",
						kv.Latency(start),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "query service close failed",
						kv.Latency(start),
						kv.Error(info.Error),
						kv.Version(),
					)
				}
			}
		},
		OnPoolTry: func(info trace.QueryPoolTryStartInfo) func(trace.QueryPoolTryDoneInfo) {
			if d.Details()&trace.QueryPoolEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "pool", "try")
			l.Log(ctx, "query service starting pool try attempt...")
			start := time.Now()

			return func(info trace.QueryPoolTryDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "query service pool try done",
						kv.Latency(start),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "query service pool try failed",
						kv.Latency(start),
						kv.Error(info.Error),
						kv.Version(),
					)
				}
			}
		},
		OnPoolWith: func(info trace.QueryPoolWithStartInfo) func(trace.QueryPoolWithDoneInfo) {
			if d.Details()&trace.QueryPoolEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, DEBUG, "ydb", "query", "pool", "with")
			l.Log(ctx, "staring ydb query pool with...")
			start := time.Now()

			return func(info trace.QueryPoolWithDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "query pool with done",
						kv.Latency(start),
						kv.Int("Attempts", info.Attempts),
					)
				} else {
					lvl := ERROR
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "query pool with failed",
						kv.Latency(start),
						kv.Error(info.Error),
						kv.Int("Attempts", info.Attempts),
						kv.Version(),
					)
				}
			}
		},
		OnPoolPut: func(info trace.QueryPoolPutStartInfo) func(trace.QueryPoolPutDoneInfo) {
			if d.Details()&trace.QueryPoolEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "pool", "put")
			l.Log(ctx, "starting ydb query pool put...")
			start := time.Now()

			return func(info trace.QueryPoolPutDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "query pool put done",
						kv.Latency(start),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "query pool put failed",
						kv.Latency(start),
						kv.Error(info.Error),
						kv.Version(),
					)
				}
			}
		},
		OnPoolGet: func(info trace.QueryPoolGetStartInfo) func(trace.QueryPoolGetDoneInfo) {
			if d.Details()&trace.QueryPoolEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "pool", "get")
			l.Log(ctx, "staring ydb query pool get...")
			start := time.Now()

			return func(info trace.QueryPoolGetDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "query pool get done",
						kv.Latency(start),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "query pool get failed",
						kv.Latency(start),
						kv.Error(info.Error),
						kv.Version(),
					)
				}
			}
		},
		OnDo: func(info trace.QueryDoStartInfo) func(trace.QueryDoDoneInfo) {
			if d.Details()&trace.QueryEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "do")
			l.Log(ctx, "staring ydb query do...")
			start := time.Now()

			return func(info trace.QueryDoDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "query do done",
						kv.Latency(start),
						kv.Int("attempts", info.Attempts),
					)
				} else {
					lvl := ERROR
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "query do failed",
						kv.Latency(start),
						kv.Error(info.Error),
						kv.Int("attempts", info.Attempts),
						kv.Version(),
					)
				}
			}
		},
		OnDoTx: func(info trace.QueryDoTxStartInfo) func(trace.QueryDoTxDoneInfo) {
			if d.Details()&trace.QueryEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "do", "tx")
			l.Log(ctx, "starting ydb query doTx...")
			start := time.Now()

			return func(info trace.QueryDoTxDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "query doTx done",
						kv.Latency(start),
						kv.Int("attempts", info.Attempts),
					)
				} else {
					lvl := ERROR
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "query doTx failed",
						kv.Latency(start),
						kv.Error(info.Error),
						kv.Int("attempts", info.Attempts),
						kv.Version(),
					)
				}
			}
		},
		OnExec: func(info trace.QueryExecStartInfo) func(trace.QueryExecDoneInfo) {
			if d.Details()&trace.QueryEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "exec")
			l.Log(ctx, "starting ydb query exec...")
			start := time.Now()

			return func(info trace.QueryExecDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "query exec done",
						kv.Latency(start),
					)
				} else {
					lvl := ERROR
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "query exec failed",
						kv.Latency(start),
						kv.Error(info.Error),
						kv.Version(),
					)
				}
			}
		},
		OnQuery: func(info trace.QueryQueryStartInfo) func(trace.QueryQueryDoneInfo) {
			if d.Details()&trace.QueryEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "query")
			l.Log(ctx, "staring ydb query query...")
			start := time.Now()

			return func(info trace.QueryQueryDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "query done",
						kv.Latency(start),
					)
				} else {
					lvl := ERROR
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "query failed",
						kv.Latency(start),
						kv.Error(info.Error),
						kv.Version(),
					)
				}
			}
		},
		OnQueryRow: func(info trace.QueryQueryRowStartInfo) func(trace.QueryQueryRowDoneInfo) {
			if d.Details()&trace.QueryEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "query", "row")
			l.Log(ctx, "staring ydb query row done...")
			start := time.Now()

			return func(info trace.QueryQueryRowDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "query row done",
						kv.Latency(start),
					)
				} else {
					lvl := ERROR
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "query row failed",
						kv.Latency(start),
						kv.Error(info.Error),
						kv.Version(),
					)
				}
			}
		},
		OnQueryResultSet: func(info trace.QueryQueryResultSetStartInfo) func(trace.QueryQueryResultSetDoneInfo) {
			if d.Details()&trace.QueryEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "query", "result", "set")
			l.Log(ctx, "staring ydb query result set...")
			start := time.Now()

			return func(info trace.QueryQueryResultSetDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "query result set done",
						kv.Latency(start),
					)
				} else {
					lvl := ERROR
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "query result set failed",
						kv.Latency(start),
						kv.Error(info.Error),
						kv.Version(),
					)
				}
			}
		},
		OnSessionCreate: func(info trace.QuerySessionCreateStartInfo) func(info trace.QuerySessionCreateDoneInfo) {
			if d.Details()&trace.QuerySessionEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "session", "create")
			l.Log(ctx, "staring ydb query session create...")
			start := time.Now()

			return func(info trace.QuerySessionCreateDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "query session create done",
						kv.Latency(start),
						kv.String("session_id", info.Session.ID()),
						kv.String("session_status", info.Session.Status()),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "query session create failed",
						kv.Latency(start),
						kv.Error(info.Error),
						kv.Version(),
					)
				}
			}
		},
		OnSessionAttach: func(info trace.QuerySessionAttachStartInfo) func(info trace.QuerySessionAttachDoneInfo) {
			if d.Details()&trace.QuerySessionEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "session", "attach")
			l.Log(ctx, "staring ydb query session attach...",
				kv.String("session_id", info.Session.ID()),
				kv.String("session_status", info.Session.Status()),
			)
			start := time.Now()

			return func(info trace.QuerySessionAttachDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "query session attach done",
						kv.Latency(start),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "query session attach failed",
						kv.Latency(start),
						kv.Error(info.Error),
						kv.Version(),
					)
				}
			}
		},
		OnSessionDelete: func(info trace.QuerySessionDeleteStartInfo) func(info trace.QuerySessionDeleteDoneInfo) {
			if d.Details()&trace.QuerySessionEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "session", "delete")
			l.Log(ctx, "starting ydb query session delete...",
				kv.String("session_id", info.Session.ID()),
				kv.String("session_status", info.Session.Status()),
			)
			start := time.Now()

			return func(info trace.QuerySessionDeleteDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "query session delete done",
						kv.Latency(start),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "query session delete failed",
						kv.Latency(start),
						kv.Error(info.Error),
						kv.Version(),
					)
				}
			}
		},
		OnSessionExec: func(info trace.QuerySessionExecStartInfo) func(info trace.QuerySessionExecDoneInfo) {
			if d.Details()&trace.QuerySessionEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "session", "exec")
			l.Log(ctx, "staring ydb query session exec...",
				kv.String("SessionID", info.Session.ID()),
				kv.String("SessionStatus", info.Session.Status()),
				kv.String("Query", info.Query),
			)
			start := time.Now()

			return func(info trace.QuerySessionExecDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "query session exec done",
						kv.Latency(start),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "query session exec failed",
						kv.Latency(start),
						kv.Error(info.Error),
						kv.Version(),
					)
				}
			}
		},
		OnSessionQuery: func(info trace.QuerySessionQueryStartInfo) func(info trace.QuerySessionQueryDoneInfo) {
			if d.Details()&trace.QuerySessionEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "session", "query")
			l.Log(ctx, "staring ydb query session query...",
				kv.String("SessionID", info.Session.ID()),
				kv.String("SessionStatus", info.Session.Status()),
				kv.String("Query", info.Query),
			)
			start := time.Now()

			return func(info trace.QuerySessionQueryDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "query session query done",
						kv.Latency(start),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "query session query failed",
						kv.Latency(start),
						kv.Error(info.Error),
						kv.Version(),
					)
				}
			}
		},
		OnSessionBegin: func(info trace.QuerySessionBeginStartInfo) func(info trace.QuerySessionBeginDoneInfo) {
			if d.Details()&trace.QuerySessionEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "session", "begin")
			l.Log(ctx, "staring ydb query session begin...",
				kv.String("SessionID", info.Session.ID()),
				kv.String("SessionStatus", info.Session.Status()),
			)
			start := time.Now()

			return func(info trace.QuerySessionBeginDoneInfo) {
				if info.Error == nil {
					l.Log(WithLevel(ctx, DEBUG), "query session begin done",
						kv.Latency(start),
						kv.String("TransactionID", info.Tx.ID()),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "query session begin failed",
						kv.Latency(start),
						kv.Error(info.Error),
						kv.Version(),
					)
				}
			}
		},
		OnTxExec: func(info trace.QueryTxExecStartInfo) func(info trace.QueryTxExecDoneInfo) {
			if d.Details()&trace.QueryTransactionEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "transaction", "exec")
			l.Log(ctx, "staring ydb query transaction exec...",
				kv.String("SessionID", info.Session.ID()),
				kv.String("TransactionID", info.Tx.ID()),
				kv.String("SessionStatus", info.Session.Status()),
			)
			start := time.Now()

			return func(info trace.QueryTxExecDoneInfo) {
				if info.Error == nil {
					l.Log(WithLevel(ctx, DEBUG), "query transaction exec done",
						kv.Latency(start),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "query transaction exec failed",
						kv.Latency(start),
						kv.Error(info.Error),
						kv.Version(),
					)
				}
			}
		},
		OnTxQuery: func(info trace.QueryTxQueryStartInfo) func(info trace.QueryTxQueryDoneInfo) {
			if d.Details()&trace.QueryTransactionEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "transaction", "query")
			l.Log(ctx, "staring ydb query transaction query...",
				kv.String("SessionID", info.Session.ID()),
				kv.String("TransactionID", info.Tx.ID()),
				kv.String("SessionStatus", info.Session.Status()),
			)
			start := time.Now()

			return func(info trace.QueryTxQueryDoneInfo) {
				if info.Error == nil {
					l.Log(WithLevel(ctx, DEBUG), "query transaction query done",
						kv.Latency(start),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "query transaction query failed",
						kv.Latency(start),
						kv.Error(info.Error),
						kv.Version(),
					)
				}
			}
		},
		OnResultNew: func(info trace.QueryResultNewStartInfo) func(info trace.QueryResultNewDoneInfo) {
			if d.Details()&trace.QueryResultEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "result", "new")
			l.Log(ctx, "staring ydb query result new...")
			start := time.Now()

			return func(info trace.QueryResultNewDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "query result new done",
						kv.Latency(start),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "query result new failed",
						kv.Latency(start),
						kv.Error(info.Error),
						kv.Version(),
					)
				}
			}
		},
		OnResultNextPart: func(info trace.QueryResultNextPartStartInfo) func(info trace.QueryResultNextPartDoneInfo) {
			if d.Details()&trace.QueryResultEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "result", "next", "part")
			l.Log(ctx, "staring ydb query result next part...")
			start := time.Now()

			return func(info trace.QueryResultNextPartDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "query result next part done",
						kv.Stringer("stats", info.Stats),
						kv.Latency(start),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "query result next part failed",
						kv.Latency(start),
						kv.Error(info.Error),
						kv.Version(),
					)
				}
			}
		},
		OnResultNextResultSet: func(
			info trace.QueryResultNextResultSetStartInfo,
		) func(
			info trace.QueryResultNextResultSetDoneInfo,
		) {
			if d.Details()&trace.QueryResultEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "result", "next", "result", "set")
			l.Log(ctx, "staring ydb query result next set...")
			start := time.Now()

			return func(info trace.QueryResultNextResultSetDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "query result next set done",
						kv.Latency(start),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "query result next set failed",
						kv.Latency(start),
						kv.Error(info.Error),
						kv.Version(),
					)
				}
			}
		},
		OnResultClose: func(info trace.QueryResultCloseStartInfo) func(info trace.QueryResultCloseDoneInfo) {
			if d.Details()&trace.QueryResultEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "result", "close")
			l.Log(ctx, "staring ydb query result close...")
			start := time.Now()

			return func(info trace.QueryResultCloseDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "query result close done",
						kv.Latency(start),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "query result close failed",
						kv.Latency(start),
						kv.Error(info.Error),
						kv.Version(),
					)
				}
			}
		},
	}
}
