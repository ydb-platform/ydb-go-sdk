package spans

import (
	"errors"
	"io"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/kv"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

//nolint:funlen,gocyclo
func query(adapter Adapter) trace.Query {
	nodeID := func(session interface{ NodeID() uint32 }) int64 {
		if session != nil {
			return int64(session.NodeID())
		}

		return 0
	}

	return trace.Query{
		OnNew: func(info trace.QueryNewStartInfo) func(info trace.QueryNewDoneInfo) {
			if adapter.Details()&trace.QueryEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
			)

			return func(info trace.QueryNewDoneInfo) {
				start.End()
			}
		},
		OnClose: func(info trace.QueryCloseStartInfo) func(info trace.QueryCloseDoneInfo) {
			if adapter.Details()&trace.QueryEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
			)

			return func(info trace.QueryCloseDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnPoolNew: func(info trace.QueryPoolNewStartInfo) func(trace.QueryPoolNewDoneInfo) {
			if adapter.Details()&trace.QueryPoolEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
			)

			return func(info trace.QueryPoolNewDoneInfo) {
				start.End(
					kv.Int("Limit", info.Limit),
				)
			}
		},
		OnPoolClose: func(info trace.QueryPoolCloseStartInfo) func(trace.QueryPoolCloseDoneInfo) {
			if adapter.Details()&trace.QueryPoolEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
			)

			return func(info trace.QueryPoolCloseDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnPoolTry: func(info trace.QueryPoolTryStartInfo) func(trace.QueryPoolTryDoneInfo) {
			if adapter.Details()&trace.QueryPoolEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
			)

			return func(info trace.QueryPoolTryDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnPoolWith: func(info trace.QueryPoolWithStartInfo) func(trace.QueryPoolWithDoneInfo) {
			if adapter.Details()&trace.QueryPoolEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
			)

			return func(info trace.QueryPoolWithDoneInfo) {
				finish(
					start,
					info.Error,
					kv.Int("Attempts", info.Attempts),
				)
			}
		},
		OnPoolPut: func(info trace.QueryPoolPutStartInfo) func(trace.QueryPoolPutDoneInfo) {
			if adapter.Details()&trace.QueryPoolEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
			)

			return func(info trace.QueryPoolPutDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnPoolGet: func(info trace.QueryPoolGetStartInfo) func(trace.QueryPoolGetDoneInfo) {
			if adapter.Details()&trace.QueryPoolEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
			)

			return func(info trace.QueryPoolGetDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnDo: func(info trace.QueryDoStartInfo) func(trace.QueryDoDoneInfo) {
			if adapter.Details()&trace.QueryEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
			)

			return func(info trace.QueryDoDoneInfo) {
				finish(
					start,
					info.Error,
					kv.Int("Attempts", info.Attempts),
				)
			}
		},
		OnDoTx: func(info trace.QueryDoTxStartInfo) func(trace.QueryDoTxDoneInfo) {
			if adapter.Details()&trace.QueryEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
			)

			return func(info trace.QueryDoTxDoneInfo) {
				finish(
					start,
					info.Error,
					kv.Int("Attempts", info.Attempts),
				)
			}
		},
		OnExec: func(info trace.QueryExecStartInfo) func(info trace.QueryExecDoneInfo) {
			if adapter.Details()&trace.QueryEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
				kv.String("Query", strings.TrimSpace(info.Query)),
			)

			return func(info trace.QueryExecDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnQuery: func(info trace.QueryQueryStartInfo) func(info trace.QueryQueryDoneInfo) {
			if adapter.Details()&trace.QueryEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
				kv.String("Query", strings.TrimSpace(info.Query)),
			)

			return func(info trace.QueryQueryDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnQueryResultSet: func(info trace.QueryQueryResultSetStartInfo) func(info trace.QueryQueryResultSetDoneInfo) {
			if adapter.Details()&trace.QueryEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
				kv.String("Query", strings.TrimSpace(info.Query)),
			)

			return func(info trace.QueryQueryResultSetDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnQueryRow: func(info trace.QueryQueryRowStartInfo) func(info trace.QueryQueryRowDoneInfo) {
			if adapter.Details()&trace.QueryEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
				kv.String("Query", strings.TrimSpace(info.Query)),
			)

			return func(info trace.QueryQueryRowDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnSessionCreate: func(info trace.QuerySessionCreateStartInfo) func(info trace.QuerySessionCreateDoneInfo) {
			if adapter.Details()&trace.QuerySessionEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
			)

			return func(info trace.QuerySessionCreateDoneInfo) {
				finish(
					start,
					info.Error,
					kv.String("SessionID", safeID(info.Session)),
					kv.String("SessionStatus", safeStatus(info.Session)),
					kv.Int64("NodeID", nodeID(info.Session)),
				)
			}
		},
		OnSessionAttach: func(info trace.QuerySessionAttachStartInfo) func(info trace.QuerySessionAttachDoneInfo) {
			if adapter.Details()&trace.QuerySessionEvents == 0 {
				return nil
			}

			ctx := *info.Context
			call := info.Call.String()

			return func(info trace.QuerySessionAttachDoneInfo) {
				if info.Error == nil {
					logToParentSpan(adapter, ctx, call)
				} else if errors.Is(info.Error, io.EOF) {
					logToParentSpan(adapter, ctx, call+" => io.EOF")
				} else {
					logToParentSpanError(adapter, ctx, info.Error)
				}
			}
		},
		OnSessionDelete: func(info trace.QuerySessionDeleteStartInfo) func(info trace.QuerySessionDeleteDoneInfo) {
			if adapter.Details()&trace.QuerySessionEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
			)

			return func(info trace.QuerySessionDeleteDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnSessionExec: func(info trace.QuerySessionExecStartInfo) func(info trace.QuerySessionExecDoneInfo) {
			if adapter.Details()&trace.QuerySessionEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
				kv.String("Query", strings.TrimSpace(info.Query)),
			)

			return func(info trace.QuerySessionExecDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnSessionQuery: func(info trace.QuerySessionQueryStartInfo) func(info trace.QuerySessionQueryDoneInfo) {
			if adapter.Details()&trace.QuerySessionEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
				kv.String("Query", strings.TrimSpace(info.Query)),
			)

			return func(info trace.QuerySessionQueryDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnSessionQueryResultSet: func(
			info trace.QuerySessionQueryResultSetStartInfo,
		) func(info trace.QuerySessionQueryResultSetDoneInfo) {
			if adapter.Details()&trace.QuerySessionEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
				kv.String("Query", strings.TrimSpace(info.Query)),
			)

			return func(info trace.QuerySessionQueryResultSetDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnSessionQueryRow: func(info trace.QuerySessionQueryRowStartInfo) func(info trace.QuerySessionQueryRowDoneInfo) {
			if adapter.Details()&trace.QuerySessionEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
				kv.String("Query", strings.TrimSpace(info.Query)),
			)

			return func(info trace.QuerySessionQueryRowDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnSessionBegin: func(info trace.QuerySessionBeginStartInfo) func(info trace.QuerySessionBeginDoneInfo) {
			if adapter.Details()&trace.QuerySessionEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
			)

			return func(info trace.QuerySessionBeginDoneInfo) {
				finish(
					start,
					info.Error,
					kv.String("TransactionID", safeID(info.Tx)),
				)
			}
		},
		OnResultNew: func(info trace.QueryResultNewStartInfo) func(info trace.QueryResultNewDoneInfo) {
			if adapter.Details()&trace.QueryResultEvents == 0 {
				return nil
			}

			ctx := *info.Context
			call := info.Call.String()

			return func(info trace.QueryResultNewDoneInfo) {
				if info.Error == nil {
					logToParentSpan(adapter, ctx, call)
				} else {
					logToParentSpanError(adapter, ctx, info.Error)
				}
			}
		},
		OnResultNextPart: func(info trace.QueryResultNextPartStartInfo) func(info trace.QueryResultNextPartDoneInfo) {
			if adapter.Details()&trace.QueryResultEvents == 0 {
				return nil
			}

			ctx := *info.Context
			call := info.Call.String()

			return func(info trace.QueryResultNextPartDoneInfo) {
				if info.Error == nil {
					logToParentSpan(adapter, ctx, call)
				} else if errors.Is(info.Error, io.EOF) {
					logToParentSpan(adapter, ctx, call+" => io.EOF")
				} else {
					logToParentSpanError(adapter, ctx, info.Error)
				}
			}
		},
		OnResultNextResultSet: func(info trace.QueryResultNextResultSetStartInfo) func(
			info trace.QueryResultNextResultSetDoneInfo) {
			if adapter.Details()&trace.QueryResultEvents == 0 {
				return nil
			}

			ctx := *info.Context
			call := info.Call.String()

			return func(info trace.QueryResultNextResultSetDoneInfo) {
				if info.Error == nil {
					logToParentSpan(adapter, ctx, call)
				} else if errors.Is(info.Error, io.EOF) {
					logToParentSpan(adapter, ctx, call+" => io.EOF")
				} else {
					logToParentSpanError(adapter, ctx, info.Error)
				}
			}
		},
		OnResultClose: func(info trace.QueryResultCloseStartInfo) func(info trace.QueryResultCloseDoneInfo) {
			if adapter.Details()&trace.QueryResultEvents == 0 {
				return nil
			}

			ctx := *info.Context
			call := info.Call.String()

			return func(info trace.QueryResultCloseDoneInfo) {
				if info.Error == nil {
					logToParentSpan(adapter, ctx, call)
				} else {
					logToParentSpanError(adapter, ctx, info.Error)
				}
			}
		},
		OnTxExec: func(info trace.QueryTxExecStartInfo) func(info trace.QueryTxExecDoneInfo) {
			if adapter.Details()&trace.QueryTransactionEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
				kv.String("Query", strings.TrimSpace(info.Query)),
			)

			return func(info trace.QueryTxExecDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnTxQuery: func(info trace.QueryTxQueryStartInfo) func(info trace.QueryTxQueryDoneInfo) {
			if adapter.Details()&trace.QueryTransactionEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
				kv.String("Query", strings.TrimSpace(info.Query)),
			)

			return func(info trace.QueryTxQueryDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnTxQueryResultSet: func(info trace.QueryTxQueryResultSetStartInfo) func(info trace.QueryTxQueryResultSetDoneInfo) {
			if adapter.Details()&trace.QueryTransactionEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
				kv.String("Query", strings.TrimSpace(info.Query)),
			)

			return func(info trace.QueryTxQueryResultSetDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnTxQueryRow: func(info trace.QueryTxQueryRowStartInfo) func(info trace.QueryTxQueryRowDoneInfo) {
			if adapter.Details()&trace.QueryTransactionEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
				kv.String("Query", strings.TrimSpace(info.Query)),
			)

			return func(info trace.QueryTxQueryRowDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
	}
}
