package xcontext

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var errCancelWithNilError = fmt.Errorf("nil error: %w", context.Canceled)

// CancelErrFunc use for cancel with wrap with specific error
// if err == nil CancelErrFunc will panic for prevent
// call cancel, then ctx.Err() == nil
type CancelErrFunc func(err error)

func WithErrCancel(ctx context.Context) (resCtx context.Context, cancel CancelErrFunc) {
	res := &ctxError{}
	res.ctx, res.ctxCancel = context.WithCancel(ctx)
	return res, res.cancel
}

type ctxError struct {
	ctx       context.Context
	ctxCancel context.CancelFunc

	m   sync.Mutex
	err error
}

func (c *ctxError) Deadline() (deadline time.Time, ok bool) {
	return c.ctx.Deadline()
}

func (c *ctxError) Done() <-chan struct{} {
	return c.ctx.Done()
}

func (c *ctxError) Err() error {
	c.m.Lock()
	defer c.m.Unlock()

	return c.errUnderLock()
}

func (c *ctxError) errUnderLock() error {
	if c.err == nil {
		c.err = c.ctx.Err()
	}

	return c.err

}

func (c *ctxError) Value(key interface{}) interface{} {
	return c.ctx.Value(key)
}

func (c *ctxError) cancel(err error) {
	c.m.Lock()
	defer c.m.Unlock()

	if err == nil {
		err = xerrors.WithStackTrace(errCancelWithNilError)
	}

	if c.errUnderLock() == nil {
		c.err = err
	}

	c.ctxCancel()
}
