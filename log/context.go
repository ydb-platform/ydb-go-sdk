package log

import "context"

type (
	ctxLevelKey struct{}
	ctxNamesKey struct{}
)

func WithLevel(ctx context.Context, lvl Level) context.Context {
	return context.WithValue(ctx, ctxLevelKey{}, lvl)
}

func LevelFromContext(ctx context.Context) Level {
	v, _ := ctx.Value(ctxLevelKey{}).(Level)

	return v
}

func WithNames(ctx context.Context, names ...string) context.Context {
	return context.WithValue(ctx, ctxNamesKey{}, append(NamesFromContext(ctx), names...))
}

func NamesFromContext(ctx context.Context) []string {
	v, _ := ctx.Value(ctxNamesKey{}).([]string)
	if v == nil {
		return []string{}
	}

	return v
}

func with(ctx context.Context, lvl Level, names ...string) context.Context {
	return WithLevel(WithNames(ctx, names...), lvl)
}
