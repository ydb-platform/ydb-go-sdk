package trace

// go:generate gtrace

import (
	"context"
	"time"
)

type (
	// gtrace:gen
	// gtrace:set Shortcut
	Retry struct {
		OnRetry func(RetryLoopStartInfo) func(RetryLoopDoneInfo)
	}
	RetryLoopStartInfo struct {
		Context context.Context
	}
	RetryLoopDoneInfo struct {
		Context context.Context
		Latency time.Duration
		Err     error
	}
)
