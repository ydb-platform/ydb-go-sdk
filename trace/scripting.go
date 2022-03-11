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
		OnExecute       func(ExecuteStartInfo) func(ExecuteDoneInfo)
		OnStreamExecute func(StreamExecuteStartInfo) func(StreamExecuteIntermediateInfo) func(StreamExecuteDoneInfo)
		OnExplain       func(info ExplainQueryStartInfo) func(doneInfo ExplainQueryDoneInfo)
	}
	ExecuteStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context    *context.Context
		Query      string
		Parameters queryParameters
	}
	ExecuteDoneInfo struct {
		Result result
		Error  error
	}
	StreamExecuteStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context    *context.Context
		Query      string
		Parameters queryParameters
	}
	StreamExecuteIntermediateInfo struct {
		Error error
	}
	StreamExecuteDoneInfo struct {
		Error error
	}
)
