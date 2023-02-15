package xcontext

import "context"

type localDcKey struct{}

func WithLocalDC(ctx context.Context, dc string) context.Context {
	return context.WithValue(ctx, localDcKey{}, dc)
}

func ExtractLocalDC(ctx context.Context) string {
	if val := ctx.Value(localDcKey{}); val != nil {
		return val.(string)
	}
	return ""
}
