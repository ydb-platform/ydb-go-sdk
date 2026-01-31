package tx

import (
	"context"
)

type (
	ctxTxControlKey     struct{}
	ctxTxControlHookKey struct{}
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

// WithCommitTx returns a new context with the commitTx flag set.
// This allows requesting commit along with the query execution in database/sql transactions.
func WithCommitTx(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxCommitTxKey{}, true)
}

// CommitTxFromContext returns true if commitTx flag is set in the context.
func CommitTxFromContext(ctx context.Context) bool {
	v, ok := ctx.Value(ctxCommitTxKey{}).(bool)

	return ok && v
}
