package table

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

type (
	ctxTransactionControlKey struct{}
	ctxTxControlHookKey      struct{}

	txControlHook func(txControl *table.TransactionControl)
)

func WithTxControlHook(ctx context.Context, hook txControlHook) context.Context {
	return context.WithValue(ctx, ctxTxControlHookKey{}, hook)
}

func WithTxControl(ctx context.Context, txc *table.TransactionControl) context.Context {
	return context.WithValue(ctx, ctxTransactionControlKey{}, txc)
}

func txControl(ctx context.Context, defaultTxControl *table.TransactionControl) (txControl *table.TransactionControl) {
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

func (c *Conn) dataQueryOptions(ctx context.Context) []options.ExecuteDataQueryOption {
	if conn.IsPreparedStatement(ctx) {
		return append(c.dataOpts, options.WithKeepInCache(true))
	}

	return c.dataOpts
}
