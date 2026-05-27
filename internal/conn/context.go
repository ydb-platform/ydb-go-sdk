package conn

import "context"

type (
	ctxNoWrappingKey struct{}
	ctxBan           struct{}
	onBan            func(cause error)
)

func WithoutWrapping(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxNoWrappingKey{}, true)
}

func UseWrapping(ctx context.Context) bool {
	b, ok := ctx.Value(ctxNoWrappingKey{}).(bool)

	return !ok || !b
}

func WithBanCallback(ctx context.Context, callback func(cause error)) context.Context {
	if callback == nil {
		return ctx
	}

	return context.WithValue(ctx, ctxBan{}, onBan(callback))
}

func Ban(ctx context.Context, cause error) {
	if onBan, ok := ctx.Value(ctxBan{}).(onBan); ok {
		onBan(cause)
	}
}
