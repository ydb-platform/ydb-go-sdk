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
		Context  context.Context
		Latency  time.Duration
		Attempts int
	}
)

func OnRetry(ctx context.Context) func(ctx context.Context, latency time.Duration, attempts int) {
	onStart := ContextRetryTrace(ctx).OnRetry
	var onDone func(RetryLoopDoneInfo)
	if onStart != nil {
		onDone = onStart(RetryLoopStartInfo{Context: ctx})
	}
	if onDone == nil {
		onDone = func(info RetryLoopDoneInfo) {}
	}
	return func(ctx context.Context, latency time.Duration, attempts int) {
		onDone(RetryLoopDoneInfo{
			Context:  ctx,
			Latency:  latency,
			Attempts: attempts,
		})
	}
}
