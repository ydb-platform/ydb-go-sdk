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

func Retry(adapter Adapter) (t trace.Retry) {
	t.OnRetry = func(info trace.RetryLoopStartInfo) func(trace.RetryLoopDoneInfo) {
		if adapter.Details()&trace.RetryEvents != 0 && isTraceRetry(*info.Context) { //nolint:nestif
			operationName := info.Label
			if operationName == "" {
				operationName = info.Call.String()
			}
			if functionID := functionID(*info.Context); functionID != "" {
				operationName = functionID
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				operationName,
				kv.Bool("idempotent", info.Idempotent),
			)
			if info.NestedCall {
				start.Warn(errNestedCall)
			}
			ctx := *info.Context

			return func(info trace.RetryLoopDoneInfo) {
				fields := []KeyValue{
					kv.Int("attempts", info.Attempts),
				}
				if fieldsFromStore := fieldsFromStore(ctx); len(fieldsFromStore) > 0 {
					fields = append(fields, fieldsFromStore...)
				}
				if info.Error != nil {
					start.Error(info.Error)
				}
				start.End(fields...)
			}
		}

		return nil
	}

	return t
}
