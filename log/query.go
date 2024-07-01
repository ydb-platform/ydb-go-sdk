package log

import (
	"time"

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
			l.Log(ctx, "start")
			start := time.Now()

			return func(info trace.QueryNewDoneInfo) {
				l.Log(WithLevel(ctx, INFO), "done",
					latencyField(start),
				)
			}
		},
		OnClose: func(info trace.QueryCloseStartInfo) func(info trace.QueryCloseDoneInfo) {
			if d.Details()&trace.QueryEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "close")
			l.Log(ctx, "start")
			start := time.Now()

			return func(info trace.QueryCloseDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						latencyField(start),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "failed",
						latencyField(start),
						Error(info.Error),
						versionField(),
					)
				}
			}
		},
		OnPoolNew: func(info trace.QueryPoolNewStartInfo) func(trace.QueryPoolNewDoneInfo) {
			if d.Details()&trace.QueryPoolEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "pool", "new")
			l.Log(ctx, "start")
			start := time.Now()

			return func(info trace.QueryPoolNewDoneInfo) {
				l.Log(WithLevel(ctx, INFO), "done",
					latencyField(start),
					Int("Limit", info.Limit),
				)
			}
		},
		OnPoolClose: func(info trace.QueryPoolCloseStartInfo) func(trace.QueryPoolCloseDoneInfo) {
			if d.Details()&trace.QueryPoolEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "pool", "close")
			l.Log(ctx, "start")
			start := time.Now()

			return func(info trace.QueryPoolCloseDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						latencyField(start),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "failed",
						latencyField(start),
						Error(info.Error),
						versionField(),
					)
				}
			}
		},
		OnPoolTry: func(info trace.QueryPoolTryStartInfo) func(trace.QueryPoolTryDoneInfo) {
			if d.Details()&trace.QueryPoolEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "pool", "try")
			l.Log(ctx, "start")
			start := time.Now()

			return func(info trace.QueryPoolTryDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						latencyField(start),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "failed",
						latencyField(start),
						Error(info.Error),
						versionField(),
					)
				}
			}
		},
		OnPoolWith: func(info trace.QueryPoolWithStartInfo) func(trace.QueryPoolWithDoneInfo) {
			if d.Details()&trace.QueryPoolEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, DEBUG, "ydb", "query", "pool", "with")
			l.Log(ctx, "start")
			start := time.Now()

			return func(info trace.QueryPoolWithDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						latencyField(start),
						Int("Attempts", info.Attempts),
					)
				} else {
					lvl := ERROR
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "failed",
						latencyField(start),
						Error(info.Error),
						Int("Attempts", info.Attempts),
						versionField(),
					)
				}
			}
		},
		OnPoolPut: func(info trace.QueryPoolPutStartInfo) func(trace.QueryPoolPutDoneInfo) {
			if d.Details()&trace.QueryPoolEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "pool", "put")
			l.Log(ctx, "start")
			start := time.Now()

			return func(info trace.QueryPoolPutDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						latencyField(start),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "failed",
						latencyField(start),
						Error(info.Error),
						versionField(),
					)
				}
			}
		},
		OnPoolGet: func(info trace.QueryPoolGetStartInfo) func(trace.QueryPoolGetDoneInfo) {
			if d.Details()&trace.QueryPoolEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "pool", "get")
			l.Log(ctx, "start")
			start := time.Now()

			return func(info trace.QueryPoolGetDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						latencyField(start),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "failed",
						latencyField(start),
						Error(info.Error),
						versionField(),
					)
				}
			}
		},
		OnDo: func(info trace.QueryDoStartInfo) func(trace.QueryDoDoneInfo) {
			if d.Details()&trace.QueryEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "do")
			l.Log(ctx, "start")
			start := time.Now()

			return func(info trace.QueryDoDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						latencyField(start),
						Int("attempts", info.Attempts),
					)
				} else {
					lvl := ERROR
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "failed",
						latencyField(start),
						Error(info.Error),
						Int("attempts", info.Attempts),
						versionField(),
					)
				}
			}
		},
		OnDoTx: func(info trace.QueryDoTxStartInfo) func(trace.QueryDoTxDoneInfo) {
			if d.Details()&trace.QueryEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "do", "tx")
			l.Log(ctx, "start")
			start := time.Now()

			return func(info trace.QueryDoTxDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						latencyField(start),
						Int("attempts", info.Attempts),
					)
				} else {
					lvl := ERROR
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "failed",
						latencyField(start),
						Error(info.Error),
						Int("attempts", info.Attempts),
						versionField(),
					)
				}
			}
		},
		OnReadRow: func(info trace.QueryReadRowStartInfo) func(trace.QueryReadRowDoneInfo) {
			if d.Details()&trace.QueryEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "read", "row")
			l.Log(ctx, "start")
			start := time.Now()

			return func(info trace.QueryReadRowDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						latencyField(start),
					)
				} else {
					lvl := ERROR
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "failed",
						latencyField(start),
						Error(info.Error),
						versionField(),
					)
				}
			}
		},
		OnReadResultSet: func(info trace.QueryReadResultSetStartInfo) func(trace.QueryReadResultSetDoneInfo) {
			if d.Details()&trace.QueryEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "read", "result", "set")
			l.Log(ctx, "start")
			start := time.Now()

			return func(info trace.QueryReadResultSetDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						latencyField(start),
					)
				} else {
					lvl := ERROR
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "failed",
						latencyField(start),
						Error(info.Error),
						versionField(),
					)
				}
			}
		},
		OnSessionCreate: func(info trace.QuerySessionCreateStartInfo) func(info trace.QuerySessionCreateDoneInfo) {
			if d.Details()&trace.QuerySessionEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "session", "create")
			l.Log(ctx, "start")
			start := time.Now()

			return func(info trace.QuerySessionCreateDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						latencyField(start),
						String("session_id", info.Session.ID()),
						String("session_status", info.Session.Status()),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "done",
						latencyField(start),
						Error(info.Error),
						versionField(),
					)
				}
			}
		},
		OnSessionAttach: func(info trace.QuerySessionAttachStartInfo) func(info trace.QuerySessionAttachDoneInfo) {
			if d.Details()&trace.QuerySessionEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "session", "attach")
			l.Log(ctx, "start",
				String("session_id", info.Session.ID()),
				String("session_status", info.Session.Status()),
			)
			start := time.Now()

			return func(info trace.QuerySessionAttachDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						latencyField(start),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "failed",
						latencyField(start),
						Error(info.Error),
						versionField(),
					)
				}
			}
		},
		OnSessionDelete: func(info trace.QuerySessionDeleteStartInfo) func(info trace.QuerySessionDeleteDoneInfo) {
			if d.Details()&trace.QuerySessionEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "session", "delete")
			l.Log(ctx, "start",
				String("session_id", info.Session.ID()),
				String("session_status", info.Session.Status()),
			)
			start := time.Now()

			return func(info trace.QuerySessionDeleteDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						latencyField(start),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "failed",
						latencyField(start),
						Error(info.Error),
						versionField(),
					)
				}
			}
		},
		OnSessionExecute: func(info trace.QuerySessionExecuteStartInfo) func(info trace.QuerySessionExecuteDoneInfo) {
			if d.Details()&trace.QuerySessionEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "session", "execute")
			l.Log(ctx, "start",
				String("SessionID", info.Session.ID()),
				String("SessionStatus", info.Session.Status()),
				String("Query", info.Query),
			)
			start := time.Now()

			return func(info trace.QuerySessionExecuteDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						latencyField(start),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "failed",
						latencyField(start),
						Error(info.Error),
						versionField(),
					)
				}
			}
		},
		OnSessionBegin: func(info trace.QuerySessionBeginStartInfo) func(info trace.QuerySessionBeginDoneInfo) {
			if d.Details()&trace.QuerySessionEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "session", "begin")
			l.Log(ctx, "start",
				String("SessionID", info.Session.ID()),
				String("SessionStatus", info.Session.Status()),
			)
			start := time.Now()

			return func(info trace.QuerySessionBeginDoneInfo) {
				if info.Error == nil {
					l.Log(WithLevel(ctx, DEBUG), "done",
						latencyField(start),
						String("TransactionID", info.Tx.ID()),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "failed",
						latencyField(start),
						Error(info.Error),
						versionField(),
					)
				}
			}
		},
		OnTxExecute: func(info trace.QueryTxExecuteStartInfo) func(info trace.QueryTxExecuteDoneInfo) {
			if d.Details()&trace.QueryTransactionEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "transaction", "execute")
			l.Log(ctx, "start",
				String("SessionID", info.Session.ID()),
				String("TransactionID", info.Tx.ID()),
				String("SessionStatus", info.Session.Status()),
			)
			start := time.Now()

			return func(info trace.QueryTxExecuteDoneInfo) {
				if info.Error == nil {
					l.Log(WithLevel(ctx, DEBUG), "done",
						latencyField(start),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "failed",
						latencyField(start),
						Error(info.Error),
						versionField(),
					)
				}
			}
		},
		OnResultNew: func(info trace.QueryResultNewStartInfo) func(info trace.QueryResultNewDoneInfo) {
			if d.Details()&trace.QueryResultEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "result", "new")
			l.Log(ctx, "start")
			start := time.Now()

			return func(info trace.QueryResultNewDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						latencyField(start),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "failed",
						latencyField(start),
						Error(info.Error),
						versionField(),
					)
				}
			}
		},
		OnResultNextPart: func(info trace.QueryResultNextPartStartInfo) func(info trace.QueryResultNextPartDoneInfo) {
			if d.Details()&trace.QueryResultEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "result", "next", "part")
			l.Log(ctx, "start")
			start := time.Now()

			return func(info trace.QueryResultNextPartDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						Stringer("stats", info.Stats),
						latencyField(start),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "failed",
						latencyField(start),
						Error(info.Error),
						versionField(),
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
			l.Log(ctx, "start")
			start := time.Now()

			return func(info trace.QueryResultNextResultSetDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						latencyField(start),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "failed",
						latencyField(start),
						Error(info.Error),
						versionField(),
					)
				}
			}
		},
		OnResultClose: func(info trace.QueryResultCloseStartInfo) func(info trace.QueryResultCloseDoneInfo) {
			if d.Details()&trace.QueryResultEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "result", "close")
			l.Log(ctx, "start")
			start := time.Now()

			return func(info trace.QueryResultCloseDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						latencyField(start),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "failed",
						latencyField(start),
						Error(info.Error),
						versionField(),
					)
				}
			}
		},
		OnResultSetNextRow: func(info trace.QueryResultSetNextRowStartInfo) func(info trace.QueryResultSetNextRowDoneInfo) {
			if d.Details()&trace.QueryResultEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "result", "set", "next", "row")
			l.Log(ctx, "start")
			start := time.Now()

			return func(info trace.QueryResultSetNextRowDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						latencyField(start),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "failed",
						latencyField(start),
						Error(info.Error),
						versionField(),
					)
				}
			}
		},
		OnRowScan: func(info trace.QueryRowScanStartInfo) func(info trace.QueryRowScanDoneInfo) {
			if d.Details()&trace.QueryResultEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "row", "scan")
			l.Log(ctx, "start")
			start := time.Now()

			return func(info trace.QueryRowScanDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						latencyField(start),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "failed",
						latencyField(start),
						Error(info.Error),
						versionField(),
					)
				}
			}
		},
		OnRowScanNamed: func(info trace.QueryRowScanNamedStartInfo) func(info trace.QueryRowScanNamedDoneInfo) {
			if d.Details()&trace.QueryResultEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "row", "scan", "named")
			l.Log(ctx, "start")
			start := time.Now()

			return func(info trace.QueryRowScanNamedDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						latencyField(start),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "failed",
						latencyField(start),
						Error(info.Error),
						versionField(),
					)
				}
			}
		},
		OnRowScanStruct: func(info trace.QueryRowScanStructStartInfo) func(info trace.QueryRowScanStructDoneInfo) {
			if d.Details()&trace.QueryResultEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "query", "row", "scan", "struct")
			l.Log(ctx, "start")
			start := time.Now()

			return func(info trace.QueryRowScanStructDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						latencyField(start),
					)
				} else {
					lvl := WARN
					if !xerrors.IsYdb(info.Error) {
						lvl = DEBUG
					}
					l.Log(WithLevel(ctx, lvl), "failed",
						latencyField(start),
						Error(info.Error),
						versionField(),
					)
				}
			}
		},
	}
}
