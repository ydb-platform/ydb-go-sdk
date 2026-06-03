package trace

import (
	"context"
	"time"
)

type (
	// Retry specified trace of retry call activity.
	// gtrace:gen
	// gtrace:out internal/retry/gtrace
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	Retry struct {
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnRetry func(RetryLoopStartInfo) func(RetryLoopDoneInfo)

		// OnRetryAttempt is fired once per retry attempt, including the first
		// one. The reported Backoff is the duration that was waited *before*
		// this attempt was started (zero for the first attempt).
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnRetryAttempt func(RetryAttemptStartInfo) func(RetryAttemptDoneInfo)
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	RetryLoopStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context

		Call       Call
		Label      string
		Idempotent bool

		NestedCall bool // a sign for detect Retry calls inside head Retry
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	RetryLoopDoneInfo struct {
		Attempts int
		Error    error
	}

	// RetryAttemptStartInfo carries the per-attempt context for retry tracing.
	//
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	RetryAttemptStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context

		Call       Call
		Label      string
		Idempotent bool

		// Attempt is the 1-based attempt number.
		Attempt int

		// Backoff is the duration that was waited before this attempt was
		// started (always zero for the first attempt).
		Backoff time.Duration
	}

	// RetryAttemptDoneInfo carries the per-attempt outcome.
	//
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	RetryAttemptDoneInfo struct {
		Error error
	}
)
