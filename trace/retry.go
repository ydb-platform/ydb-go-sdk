package trace

// tool gtrace used from ./cmd/gtrace

//go:generate gtrace

import (
	"context"
)

type (
	// Retry specified trace of retry call activity.
	// gtrace:gen
	Retry struct {
		OnRetry func(RetryLoopStartInfo) func(RetryLoopIntermediateInfo) func(RetryLoopDoneInfo)
	}
	RetryLoopStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context    *context.Context
		ID         string
		Idempotent bool
	}
	RetryLoopIntermediateInfo struct {
		Error error
	}
	RetryLoopDoneInfo struct {
		Attempts int
		Error    error
	}
)
