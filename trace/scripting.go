package trace

// tool gtrace used from ./internal/cmd/gtrace

//go:generate gtrace

import (
	"context"
)

type (
	// Scripting specified trace of scripting client activity.
	// gtrace:gen
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	Scripting struct {
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnExecute func(ScriptingExecuteStartInfo) func(ScriptingExecuteDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnStreamExecute func(
			ScriptingStreamExecuteStartInfo,
		) func(
			ScriptingStreamExecuteIntermediateInfo,
		) func(
			ScriptingStreamExecuteDoneInfo,
		)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnExplain func(ScriptingExplainStartInfo) func(ScriptingExplainDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnClose func(ScriptingCloseStartInfo) func(ScriptingCloseDoneInfo)
	}
	scriptingQueryParameters interface {
		String() string
	}
	scriptingResultErr interface {
		Err() error
	}
	scriptingResult interface {
		scriptingResultErr
		ResultSetCount() int
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	ScriptingExecuteStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context    *context.Context
		Call       call
		Query      string
		Parameters scriptingQueryParameters
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	ScriptingExecuteDoneInfo struct {
		Result scriptingResult
		Error  error
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	ScriptingStreamExecuteStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context    *context.Context
		Call       call
		Query      string
		Parameters scriptingQueryParameters
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	ScriptingStreamExecuteIntermediateInfo struct {
		Error error
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	ScriptingStreamExecuteDoneInfo struct {
		Error error
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	ScriptingExplainStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Query   string
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	ScriptingExplainDoneInfo struct {
		Plan  string
		Error error
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	ScriptingCloseStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	ScriptingCloseDoneInfo struct {
		Error error
	}
)
