package tx

import (
	"context"
)

type (
	ctxTxControlKey     struct{}
	ctxTxControlHookKey struct{}
	ctxLazyTxKey        struct{}
	ctxCommitTxKey      struct{}

	txControlHook func(txControl *Control)
)

func WithTxControlHook(ctx context.Context, hook txControlHook) context.Context {
	return context.WithValue(ctx, ctxTxControlHookKey{}, hook)
}

func WithTxControl(ctx context.Context, txControl *Control) context.Context {
	return context.WithValue(ctx, ctxTxControlKey{}, txControl)
}

func ControlFromContext(ctx context.Context, defaultTxControl *Control) (txControl *Control) {
	defer func() {
		if hook, has := ctx.Value(ctxTxControlHookKey{}).(txControlHook); has && hook != nil {
			hook(txControl)
		}
	}()
	if txc, ok := ctx.Value(ctxTxControlKey{}).(*Control); ok {
		return txc
	}

	return defaultTxControl
}

func WithLazyTx(ctx context.Context, lazyTx bool) context.Context {
	return context.WithValue(ctx, ctxLazyTxKey{}, lazyTx)
}

func LazyTxFromContext(ctx context.Context, defaultValue bool) bool {
	if v, ok := ctx.Value(ctxLazyTxKey{}).(bool); ok {
		return v
	}

	return defaultValue
}

func WithCommitTx(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxCommitTxKey{}, true)
}

func CommitTxFromContext(ctx context.Context) bool {
	v, ok := ctx.Value(ctxCommitTxKey{}).(bool)

	return ok && v
}
