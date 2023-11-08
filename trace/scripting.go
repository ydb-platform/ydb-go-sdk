package trace

// tool gtrace used from ./internal/cmd/gtrace

//go:generate gtrace

import (
	"context"
)

type (
	// Scripting specified trace of scripting client activity.
	// gtrace:gen
	Scripting struct {
		OnExecute       func(ScriptingExecuteStartInfo) func(ScriptingExecuteDoneInfo)
		OnStreamExecute func(
			ScriptingStreamExecuteStartInfo,
		) func(
			ScriptingStreamExecuteIntermediateInfo,
		) func(
			ScriptingStreamExecuteDoneInfo,
		)
		OnExplain func(ScriptingExplainStartInfo) func(ScriptingExplainDoneInfo)
		OnClose   func(ScriptingCloseStartInfo) func(ScriptingCloseDoneInfo)
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
	ScriptingExecuteDoneInfo struct {
		Result scriptingResult
		Error  error
	}
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
	ScriptingStreamExecuteIntermediateInfo struct {
		Error error
	}
	ScriptingStreamExecuteDoneInfo struct {
		Error error
	}
	ScriptingExplainStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Query   string
	}
	ScriptingExplainDoneInfo struct {
		Plan  string
		Error error
	}
	ScriptingCloseStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	ScriptingCloseDoneInfo struct {
		Error error
	}
)
