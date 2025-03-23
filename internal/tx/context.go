package tx

import (
	"context"
)

type (
	ctxTransactionControlKey struct{}
	ctxTxControlHookKey      struct{}

	txControlHook func(txControl *Control)
)

func WithTxControlHook(ctx context.Context, hook txControlHook) context.Context {
	return context.WithValue(ctx, ctxTxControlHookKey{}, hook)
}

func WithTxControl(ctx context.Context, txc *Control) context.Context {
	return context.WithValue(ctx, ctxTransactionControlKey{}, txc)
}

func ControlFromContext(ctx context.Context, defaultTxControl *Control) (txControl *Control) {
	defer func() {
		if hook, has := ctx.Value(ctxTxControlHookKey{}).(txControlHook); has && hook != nil {
			hook(txControl)
		}
	}()
	if txc, ok := ctx.Value(ctxTransactionControlKey{}).(*Control); ok {
		return txc
	}

	return defaultTxControl
}
