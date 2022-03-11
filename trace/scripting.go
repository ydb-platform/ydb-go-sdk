package trace

// tool gtrace used from ./cmd/gtrace

//go:generate gtrace

import (
	"context"
)

type (
	//gtrace:gen
	//gtrace:set Shortcut
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
	ScriptingExecuteStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context    *context.Context
		Query      string
		Parameters queryParameters
	}
	ScriptingExecuteDoneInfo struct {
		Result result
		Error  error
	}
	ScriptingStreamExecuteStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context    *context.Context
		Query      string
		Parameters queryParameters
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
	}
	ScriptingCloseDoneInfo struct {
		Error error
	}
)
