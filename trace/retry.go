package trace

// tool gtrace used from ./internal/cmd/gtrace

//go:generate gtrace

import (
	"context"
)

type (
	// Retry specified trace of retry call activity.
	// gtrace:gen
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	Retry struct {
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnRetry func(RetryLoopStartInfo) func(RetryLoopDoneInfo)
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	RetryLoopStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context

		Call       call
		Label      string
		Idempotent bool

		NestedCall bool // a sign for detect Retry calls inside head Retry
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	RetryLoopDoneInfo struct {
		Attempts int
		Error    error
	}
)
