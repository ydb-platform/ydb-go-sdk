package pool

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
)

type (
	Trace struct {
		OnNew    func(*NewStartInfo) func(*NewDoneInfo)
		OnClose  func(*CloseStartInfo) func(*CloseDoneInfo)
		OnTry    func(*TryStartInfo) func(*TryDoneInfo)
		OnWith   func(*WithStartInfo) func(*WithDoneInfo)
		OnPut    func(*PutStartInfo) func(*PutDoneInfo)
		OnGet    func(*GetStartInfo) func(*GetDoneInfo)
		OnChange func(ChangeInfo)
	}
	NewStartInfo struct {
		// Context make available context in trace stack.Callerback function.
		// Pointer to context provide replacement of context in trace stack.Callerback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside stack.Callerback function
		Context *context.Context
		Call    stack.Caller
	}
	NewDoneInfo struct {
		Limit int
	}
	CloseStartInfo struct {
		// Context make available context in trace stack.Callerback function.
		// Pointer to context provide replacement of context in trace stack.Callerback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside stack.Callerback function
		Context *context.Context
		Call    stack.Caller
	}
	CloseDoneInfo struct {
		Error error
	}
	TryStartInfo struct {
		// Context make available context in trace stack.Callerback function.
		// Pointer to context provide replacement of context in trace stack.Callerback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside stack.Callerback function
		Context *context.Context
		Call    stack.Caller
	}
	TryDoneInfo struct {
		Error error
	}
	WithStartInfo struct {
		// Context make available context in trace stack.Callerback function.
		// Pointer to context provide replacement of context in trace stack.Callerback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside stack.Callerback function
		Context *context.Context
		Call    stack.Caller
	}
	WithDoneInfo struct {
		Error error

		Attempts int
	}
	PutStartInfo struct {
		// Context make available context in trace stack.Callerback function.
		// Pointer to context provide replacement of context in trace stack.Callerback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside stack.Callerback function
		Context *context.Context
		Call    stack.Caller
	}
	PutDoneInfo struct {
		Error error
	}
	GetStartInfo struct {
		// Context make available context in trace stack.Callerback function.
		// Pointer to context provide replacement of context in trace stack.Callerback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside stack.Callerback function
		Context *context.Context
		Call    stack.Caller
	}
	GetDoneInfo struct {
		Error error
	}
	ChangeInfo = Stats
)
