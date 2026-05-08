package xcontext

import (
	"context"
)

var (
	noopCancel     = func() {}
	closedDoneChan = func() chan struct{} {
		ch := make(chan struct{})
		close(ch)

		return ch
	}()
)

// doneAlreadySignaledCtx is used when done is readable at WithDone entry so we
// avoid context.WithCancel and context.AfterFunc (cheaper fast path).
type doneAlreadySignaledCtx struct {
	context.Context //nolint:containedctx // thin wrapper delegating Deadline/Value
}

func (doneAlreadySignaledCtx) Done() <-chan struct{} {
	return closedDoneChan
}

func (doneAlreadySignaledCtx) Err() error {
	return context.Canceled
}

func WithDone(parent context.Context, done <-chan struct{}) (context.Context, context.CancelFunc) {
	select {
	case <-done:
		return doneAlreadySignaledCtx{Context: parent}, noopCancel
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
