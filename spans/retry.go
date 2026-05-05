package spans

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/kv"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type (
	ctxRetryFunctionIDKey struct{}
	ctxRetryFieldsKey     struct{}
	ctxNoTraceRetryKey    struct{}
	fieldsStore           struct {
		fields []kv.KeyValue
	}
)

func withFunctionID(ctx context.Context, functionID string) context.Context {
	return context.WithValue(ctx, ctxRetryFunctionIDKey{}, functionID)
}

func functionID(ctx context.Context) string {
	if functionID, has := ctx.Value(ctxRetryFunctionIDKey{}).(string); has {
		return functionID
	}

	return ""
}

func noTraceRetry(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxNoTraceRetryKey{}, true)
}

func isTraceRetry(ctx context.Context) bool {
	if noTrace, has := ctx.Value(ctxNoTraceRetryKey{}).(bool); has {
		return !noTrace
	}

	return true
}

func fieldsStoreFromContext(ctx *context.Context) *fieldsStore {
	if store, has := (*ctx).Value(ctxRetryFieldsKey{}).(*fieldsStore); has {
		return store
	}
	store := &fieldsStore{}
	*ctx = context.WithValue(*ctx, ctxRetryFieldsKey{}, store)

	return store
}

func fieldsFromStore(ctx context.Context) []kv.KeyValue {
	if holder, has := ctx.Value(ctxRetryFieldsKey{}).(*fieldsStore); has {
		return holder.fields
	}

	return nil
}

// Retry returns a trace.Retry that emits OTel-compatible retry spans.
//
// Span structure:
//
//	ydb.RunWithRetry          // INTERNAL — wraps the whole retry loop
//	  ydb.Try                 // INTERNAL — one per attempt (incl. the first)
//	    <user span(s)>        // CLIENT, e.g. ydb.ExecuteQuery, ydb.Commit
//
// Each ydb.Try span carries the ydb.retry.backoff_ms attribute equal to the
// backoff that was waited *before* that attempt was started. The first
// attempt's backoff is therefore always 0.
//
// Errors are reported through SetException-equivalent semantics:
//   - on a non-retryable error the ydb.Try span and the surrounding
//     ydb.RunWithRetry are both marked failed (db.response.status_code /
//     error.type are attached when applicable);
//   - on retries exhaustion all ydb.Try spans and ydb.RunWithRetry get the
//     terminal error;
//   - on cancellation error.type is set to the dynamic Go type name of the
//     causing error (e.g. "*errors.errorString" for context.Canceled).
func Retry(adapter Adapter) (t trace.Retry) {
	t.OnRetry = func(info trace.RetryLoopStartInfo) func(trace.RetryLoopDoneInfo) {
		if adapter.Details()&trace.RetryEvents == 0 || !isTraceRetry(*info.Context) {
			return nil
		}
		operationName := SpanNameRunWithRetry
		if functionID := functionID(*info.Context); functionID != "" {
			// Preserve a per-call label that callers may have attached via
			// retry.WithLabel / function id, useful for grouping spans.
			operationName = functionID
		} else if info.Label != "" {
			operationName = SpanNameRunWithRetry + "/" + info.Label
		}
		start := childSpanWithReplaceCtx(
			adapter,
			info.Context,
			operationName,
		)
		if info.NestedCall {
			start.Warn(errNestedCall)
		}
		ctx := *info.Context

		return func(info trace.RetryLoopDoneInfo) {
			fields := fieldsFromStore(ctx)
			if info.Error != nil {
				setSpanException(start, info.Error)
			}
			start.End(fields...)
		}
	}

	t.OnRetryAttempt = func(info trace.RetryAttemptStartInfo) func(trace.RetryAttemptDoneInfo) {
		if adapter.Details()&trace.RetryEvents == 0 || !isTraceRetry(*info.Context) {
			return nil
		}
		// The first attempt's span carries no extra tags; only retry
		// attempts (i.e. attempts that were preceded by an actual sleep)
		// get ydb.retry.backoff_ms.
		var attrs []KeyValue
		if info.Backoff > 0 {
			attrs = append(attrs, kv.Int64(AttrYDBRetryBackoffMs, info.Backoff.Milliseconds()))
		}
		start := childSpanWithReplaceCtx(
			adapter,
			info.Context,
			SpanNameTry,
			attrs...,
		)

		return func(info trace.RetryAttemptDoneInfo) {
			if info.Error != nil {
				setSpanException(start, info.Error)
			}
			start.End()
		}
	}

	return t
}
