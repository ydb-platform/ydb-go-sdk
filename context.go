package ydb

import (
	"context"
	"time"
)

type (
	ctxRetryNoIdempotentKey struct{}
)

type valueOnlyContext struct{ context.Context }

func (valueOnlyContext) Deadline() (deadline time.Time, ok bool) { return }
func (valueOnlyContext) Done() <-chan struct{}                   { return nil }
func (valueOnlyContext) Err() error                              { return nil }

// ContextWithoutDeadline helps to clear derived context from deadline
func ContextWithoutDeadline(ctx context.Context) context.Context {
	return valueOnlyContext{ctx}
}

// WithRetryNoIdempotent returns a copy of parent context with allow retry
// operations with no idempotent errors
func WithRetryNoIdempotent(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxRetryNoIdempotentKey{}, true)
}

// ContextRetryNoIdempotent returns the flag for retry with no idempotent errors
func ContextRetryNoIdempotent(ctx context.Context) bool {
	v, ok := ctx.Value(ctxRetryNoIdempotentKey{}).(bool)
	return ok && v
}

type credentialsSourceInfoContextKey struct{}

func WithCredentialsSourceInfo(ctx context.Context, sourceInfo string) context.Context {
	return context.WithValue(ctx, credentialsSourceInfoContextKey{}, sourceInfo)
}

func ContextCredentialsSourceInfo(ctx context.Context) (sourceInfo string, ok bool) {
	sourceInfo, ok = ctx.Value(credentialsSourceInfoContextKey{}).(string)
	return
}
