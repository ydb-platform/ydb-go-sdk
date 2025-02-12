package common

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

type (
	ctxPreparedStatementKey  struct{}
	ctxTransactionControlKey struct{}
	ctxTxControlHookKey      struct{}

	txControlHook func(txControl *table.TransactionControl)
)

func WithPreparedStatement(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxPreparedStatementKey{}, true)
}

func IsPreparedStatement(ctx context.Context) bool {
	_, ok := ctx.Value(ctxPreparedStatementKey{}).(bool)

	return ok
}

func WithTxControlHook(ctx context.Context, hook txControlHook) context.Context {
	return context.WithValue(ctx, ctxTxControlHookKey{}, hook)
}

func WithTxControl(ctx context.Context, txc *table.TransactionControl) context.Context {
	return context.WithValue(ctx, ctxTransactionControlKey{}, txc)
}

func TxControl(ctx context.Context, defaultTxControl *table.TransactionControl) (txControl *table.TransactionControl) {
	defer func() {
		if hook, has := ctx.Value(ctxTxControlHookKey{}).(txControlHook); has && hook != nil {
			hook(txControl)
		}
	}()
	if txc, ok := ctx.Value(ctxTransactionControlKey{}).(*table.TransactionControl); ok {
		return txc
	}

	return defaultTxControl
}
