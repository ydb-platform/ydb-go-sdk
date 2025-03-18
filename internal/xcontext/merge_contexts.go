package xcontext

import (
	"context"
	"time"
)

type MergedContexts struct {
	context.Context                 //nolint:containedctx
	BaseContext     context.Context //nolint:containedctx
	RequestContext  context.Context //nolint:containedctx
}

func (ctx MergedContexts) Deadline() (deadline time.Time, ok bool) {
	return ctx.RequestContext.Deadline()
}

func (ctx MergedContexts) Done() <-chan struct{} {
	return ctx.RequestContext.Done()
}

func (ctx MergedContexts) Err() error {
	return ctx.RequestContext.Err()
}

func (ctx MergedContexts) Value(key interface{}) interface{} {
	if ctx.RequestContext.Value(key) != nil {
		return ctx.RequestContext.Value(key)
	}

	return ctx.BaseContext.Value(key)
}

func (ctx MergedContexts) AsCtx() *context.Context {
	var ret context.Context = &ctx

	return &ret
}

func MergeContexts(req context.Context, log context.Context) MergedContexts {
	return MergedContexts{
		BaseContext:    log,
		RequestContext: req,
	}
}
