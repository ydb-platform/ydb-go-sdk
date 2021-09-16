package credentials

import "context"

type credentialsSourceInfoContextKey struct{}

func WithCredentialsSourceInfo(ctx context.Context, sourceInfo string) context.Context {
	return context.WithValue(ctx, credentialsSourceInfoContextKey{}, sourceInfo)
}

func ContextCredentialsSourceInfo(ctx context.Context) (sourceInfo string, ok bool) {
	sourceInfo, ok = ctx.Value(credentialsSourceInfoContextKey{}).(string)
	return
}
