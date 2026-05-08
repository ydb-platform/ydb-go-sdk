package xcontext

import (
	"context"
	"time"
)

var (
	noopCancel     = func() {}
	closedDoneChan = func() chan struct{} {
		ch := make(chan struct{})
		close(ch)

		return ch
	}
)

// doneAlreadySignaledCtx is used when done is readable at WithDone entry so we
// avoid context.WithCancel and context.AfterFunc (cheaper fast path).
type doneAlreadySignaledCtx struct {
	parent context.Context //nolint:containedctx // thin wrapper delegating Deadline/Value
}

func (c *doneAlreadySignaledCtx) Deadline() (deadline time.Time, ok bool) {
	return c.parent.Deadline()
}

func (c *doneAlreadySignaledCtx) Done() <-chan struct{} {
	return closedDoneChan()
}

func (c *doneAlreadySignaledCtx) Err() error {
	return context.Canceled
}

func (c *doneAlreadySignaledCtx) Value(key any) any {
	return c.parent.Value(key)
}

func WithDone(parent context.Context, done <-chan struct{}) (context.Context, context.CancelFunc) {
	select {
	case <-done:
		return &doneAlreadySignaledCtx{parent: parent}, noopCancel
	default:
		ctx, cancel := context.WithCancel(parent)

		go func() {
			select {
			case <-done:
				cancel()
			case <-ctx.Done():
			}
		}()

		return ctx, cancel
	}
}
