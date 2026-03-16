package conn

import "context"

type (
	ctxNoWrappingKey   struct{}
	ctxBanOnOverloaded struct{}
)

func BanOnOverloaded(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxBanOnOverloaded{}, true)
}

func WithoutWrapping(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxNoWrappingKey{}, true)
}

func UseWrapping(ctx context.Context) bool {
	b, ok := ctx.Value(ctxNoWrappingKey{}).(bool)

	return !ok || !b
}

func NeedBanOnOverloaded(ctx context.Context) bool {
	b, ok := ctx.Value(ctxNoWrappingKey{}).(bool)

	return !ok || !b
}
