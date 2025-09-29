package log

import (
	"context"
)

type FieldsType map[string]string

type (
	ctxLevelKey  struct{}
	ctxNamesKey  struct{}
	ctxFieldsKey struct{}
)

func WithLevel(ctx context.Context, lvl Level) context.Context {
	return context.WithValue(ctx, ctxLevelKey{}, lvl)
}

func LevelFromContext(ctx context.Context) Level {
	v, _ := ctx.Value(ctxLevelKey{}).(Level)

	return v
}

func WithNames(ctx context.Context, names ...string) context.Context {
	// trim capacity for force allocate new memory while append and prevent data race
	oldNames := NamesFromContext(ctx)
	oldNames = oldNames[:len(oldNames):len(oldNames)]

	return context.WithValue(ctx, ctxNamesKey{}, append(oldNames, names...))
}

func NamesFromContext(ctx context.Context) []string {
	v, _ := ctx.Value(ctxNamesKey{}).([]string)
	if v == nil {
		return []string{}
	}

	return v[:len(v):len(v)] // prevent re
}

func WithFields(ctx context.Context, fields FieldsType) context.Context {
	existing := FieldsFromContext(ctx)
	merged := make(FieldsType, len(existing)+len(fields))

	for k, v := range existing {
		merged[k] = v
	}

	for k, v := range fields {
		merged[k] = v
	}

	return context.WithValue(ctx, ctxFieldsKey{}, merged)
}

func FieldsFromContext(ctx context.Context) FieldsType {
	if fields, _ := ctx.Value(ctxFieldsKey{}).(FieldsType); fields != nil {
		return fields
	}

	return FieldsType{}
}

func with(ctx context.Context, lvl Level, names ...string) context.Context {
	return WithLevel(WithNames(ctx, names...), lvl)
}
