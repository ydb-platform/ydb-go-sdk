package xcontext

import (
	"context"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

func WithTimeout(ctx context.Context, t time.Duration) (context.Context, context.CancelFunc) {
	childCtx := &timeoutCtx{
		parentCtx: ctx,
		from:      xerrors.StackRecord(1),
	}
	childCtx.ctx, childCtx.ctxCancel = context.WithTimeout(ctx, t)
	return childCtx, childCtx.cancel
}

type timeoutCtx struct {
	parentCtx context.Context
	ctx       context.Context
	ctxCancel context.CancelFunc
	from      string

	m   sync.Mutex
	err error
}

func (ctx *timeoutCtx) Deadline() (deadline time.Time, ok bool) {
	return ctx.ctx.Deadline()
}

func (ctx *timeoutCtx) Done() <-chan struct{} {
	return ctx.ctx.Done()
}

func (ctx *timeoutCtx) withErrUnderLock(err error) error {
	switch err { //nolint:errorlint
	case context.DeadlineExceeded:
		ctx.err = errFrom(err, ctx.from)
	case context.Canceled:
		ctx.err = errAt(err, 2)
	default:
		ctx.err = err
	}
	return ctx.err
}

func (ctx *timeoutCtx) Err() error {
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

func (ctx *timeoutCtx) Value(key interface{}) interface{} {
	return ctx.ctx.Value(key)
}

func (ctx *timeoutCtx) cancel() {
	ctx.m.Lock()
	defer ctx.m.Unlock()

	ctx.ctxCancel()

	if ctx.err != nil {
		return
	}

	if err := ctx.parentCtx.Err(); err != nil {
		_ = ctx.withErrUnderLock(err)
	} else if err = ctx.ctx.Err(); err != nil {
		_ = ctx.withErrUnderLock(err)
	}
}
