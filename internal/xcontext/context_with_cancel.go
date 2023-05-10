package xcontext

import (
	"context"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

func WithCancel(ctx context.Context) (context.Context, context.CancelFunc) {
	childCtx := &cancelCtx{
		parentCtx: ctx,
	}
	childCtx.ctx, childCtx.ctxCancel = context.WithCancel(ctx)
	return childCtx, childCtx.cancel
}

type cancelCtx struct {
	parentCtx context.Context
	ctx       context.Context
	ctxCancel context.CancelFunc

	m   sync.Mutex
	err error
}

func (ctx *cancelCtx) Deadline() (deadline time.Time, ok bool) {
	return ctx.ctx.Deadline()
}

func (ctx *cancelCtx) Done() <-chan struct{} {
	return ctx.ctx.Done()
}

func (ctx *cancelCtx) withErrUnderLock(err error) error {
	if err == context.Canceled {
		ctx.err = xerrors.WithStackTrace(err, xerrors.WithSkipDepth(2))
	} else {
		ctx.err = err
	}
	return ctx.err
}

func (ctx *cancelCtx) Err() error {
	ctx.m.Lock()
	defer ctx.m.Unlock()

	if ctx.err != nil {
		return ctx.err
	}

	if err := ctx.parentCtx.Err(); err != nil {
		return ctx.withErrUnderLock(err)
	}

	if err := ctx.ctx.Err(); err != nil {
		return ctx.withErrUnderLock(err)
	}

	return nil
}

func (ctx *cancelCtx) Value(key interface{}) interface{} {
	return ctx.ctx.Value(key)
}

func (ctx *cancelCtx) cancel() {
	ctx.m.Lock()
	defer ctx.m.Unlock()

	if ctx.err != nil {
		return
	}

	ctx.ctxCancel()

	if err := ctx.parentCtx.Err(); err != nil {
		_ = ctx.withErrUnderLock(err)
	} else if err = ctx.ctx.Err(); err != nil {
		_ = ctx.withErrUnderLock(err)
	}
}
