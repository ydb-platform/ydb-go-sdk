package trace

//go:generate gtrace

import (
	"context"
	"time"
)

type (
	//gtrace:gen
	//gtrace:set shortcut
	RetryTrace struct {
		OnRetry func(RetryLoopStartInfo) func(RetryLoopDoneInfo)
	}
	RetryLoopStartInfo struct {
		Context context.Context
	}
	RetryLoopDoneInfo struct {
		Context context.Context
		Latency time.Duration
		Issues  []error
	}
)

func OnRetry(ctx context.Context) func(ctx context.Context, latency time.Duration, issues []error) {
	onStart := ContextRetry(ctx).OnRetry
	var onDone func(RetryLoopDoneInfo)
	if onStart != nil {
		onDone = onStart(RetryLoopStartInfo{Context: ctx})
	}
	if onDone == nil {
		onDone = func(info RetryLoopDoneInfo) {}
	}
	return func(ctx context.Context, latency time.Duration, issues []error) {
		onDone(RetryLoopDoneInfo{
			Context: ctx,
			Latency: latency,
			Issues:  issues,
		})
	}
}
