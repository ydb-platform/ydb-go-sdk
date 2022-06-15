package xcontext

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var errCancelWithNilError = cancelError{err: errors.New("cancel context with nil error")}

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
		err = cancelError{err: err}
		c.err = err
	}

	c.ctxCancel()
}

type cancelError struct {
	err error
}

func (e cancelError) Error() string {
	return e.err.Error()
}

func (e cancelError) Is(target error) bool {
	return errors.Is(e.err, target) || errors.Is(context.Canceled, target)
}

func (e cancelError) As(target interface{}) bool {
	return errors.As(e.err, target) || errors.As(context.Canceled, target)
}

func (e cancelError) Unwrap() error {
	return e.err
}
