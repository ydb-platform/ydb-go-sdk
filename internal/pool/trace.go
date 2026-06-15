package pool

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type (
	Trace[PT ItemConstraint[T], T any] struct {
		OnNew   func(ctx *context.Context, call stack.Caller) func(limit int)
		OnClose func(ctx *context.Context, call stack.Caller) func(err error)
		OnTry   func(ctx *context.Context, call stack.Caller) func(err error)
		OnWith  func(ctx *context.Context, call stack.Caller) func(attempts int, err error)
		OnPut   func(ctx *context.Context, call stack.Caller, item PT) func(err error)
		OnGet   func(ctx *context.Context, call stack.Caller) func(
			item PT,
			nodeHintInfo *trace.NodeHintInfo,
			attempts int,
			err error,
		)
		OnChange func(Stats)
	}
)
