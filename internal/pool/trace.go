package pool

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
)

type (
	Trace struct {
		OnNew    func(ctx *context.Context, call stack.Caller) func(limit int)
		OnClose  func(ctx *context.Context, call stack.Caller) func(err error)
		OnTry    func(ctx *context.Context, call stack.Caller) func(err error)
		OnWith   func(ctx *context.Context, call stack.Caller) func(attempts int, err error)
		OnPut    func(ctx *context.Context, call stack.Caller, item any) func(err error)
		OnGet    func(*GetStartInfo) func(*GetDoneInfo)
		onWait   func(*waitStartInfo) func(*waitDoneInfo)
		OnChange func(ChangeInfo)
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
		Item     any
		Attempts int
		Error    error
	}
	waitStartInfo struct{}
	waitDoneInfo  struct {
		Item  any
		Error error
	}
	WaitStartInfo struct {
		// Context make available context in trace stack.Callerback function.
		// Pointer to context provide replacement of context in trace stack.Callerback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside stack.Callerback function
		Context *context.Context
		Call    stack.Caller
	}
	WaitDoneInfo struct {
		Item  any
		Error error
	}
	ChangeInfo = Stats
)
