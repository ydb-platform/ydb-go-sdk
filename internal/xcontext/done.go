package xcontext

import (
	"context"
	"time"
)

type (
	doneCtx                <-chan struct{}
	doneAlreadySignaledCtx struct {
		context.Context //nolint:containedctx // thin wrapper delegating Deadline/Value
		err             error
	}
)

var (
	noopCancel     = func() {}
	closedDoneChan = func() <-chan struct{} {
		ch := make(chan struct{})
		close(ch)

		return ch
	}()
)

func (doneAlreadySignaledCtx) Done() <-chan struct{} {
	return closedDoneChan
}

func (ctx doneAlreadySignaledCtx) Err() error {
	return ctx.err
}

func (done doneCtx) Deadline() (deadline time.Time, ok bool) {
	return
}

func (done doneCtx) Done() <-chan struct{} {
	return done
}

func (done doneCtx) Err() error {
	select {
	case <-done:
		return context.Canceled
	default:
		return nil
	}
}

func (done doneCtx) Value(key any) any {
	return nil
}

func WithDone(parent context.Context, done <-chan struct{}) (context.Context, context.CancelFunc) {
	if parent.Err() != nil {
		return parent, noopCancel
	}

	select {
	case <-done:
		err := parent.Err()
		if err == nil {
			err = context.Canceled
		}

		return doneAlreadySignaledCtx{
			Context: parent,
			err:     err,
		}, noopCancel
	default:
		ctx, cancel := context.WithCancel(parent)

		stop := context.AfterFunc(doneCtx(done), func() {
			cancel()
		})

		return ctx, func() {
			stop()
			cancel()
		}
	}
}
