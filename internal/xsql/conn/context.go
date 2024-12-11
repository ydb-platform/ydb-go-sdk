package conn

import "context"

type ctxPreparedStatementKey struct{}

func WithPreparedStatement(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxPreparedStatementKey{}, true)
}

func IsPreparedStatement(ctx context.Context) bool {
	_, ok := ctx.Value(ctxPreparedStatementKey{}).(bool)

	return ok
}
