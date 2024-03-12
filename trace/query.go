package trace

import (
	"context"
)

// tool gtrace used from ./internal/cmd/gtrace

//go:generate gtrace

type (
	querySessionInfo interface {
		ID() string
		NodeID() int64
		Status() string
	}

	// Query specified trace of retry call activity.
	// gtrace:gen
	Query struct {
		OnDo   func(QueryDoStartInfo) func(info QueryDoIntermediateInfo) func(QueryDoDoneInfo)
		OnDoTx func(QueryDoTxStartInfo) func(info QueryDoTxIntermediateInfo) func(QueryDoTxDoneInfo)

		OnCreateSession func(QueryCreateSessionStartInfo) func(info QueryCreateSessionDoneInfo)
		OnDeleteSession func(QueryDeleteSessionStartInfo) func(info QueryDeleteSessionDoneInfo)
	}

	QueryDoStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	QueryDoIntermediateInfo struct {
		Error error
	}
	QueryDoDoneInfo struct {
		Attempts int
		Error    error
	}
	QueryDoTxStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	QueryDoTxIntermediateInfo struct {
		Error error
	}
	QueryDoTxDoneInfo struct {
		Attempts int
		Error    error
	}
	QueryCreateSessionStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	QueryCreateSessionDoneInfo struct {
		Session querySessionInfo
		Error   error
	}
	QueryDeleteSessionStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Session querySessionInfo
	}
	QueryDeleteSessionDoneInfo struct {
		Error error
	}
)
