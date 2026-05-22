package xcontext

import (
	"context"
	"time"
)

type (
	doneCtx                <-chan struct{}
	doneAlreadySignaledCtx struct {
		context.Context //nolint:containedctx // thin wrapper delegating Deadline/Value
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
	if err := ctx.Context.Err(); err != nil {
		return err
	}

	return context.Canceled
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
	select {
	case <-done:
		return doneAlreadySignaledCtx{Context: parent}, noopCancel
	default:
	}

	ctx, cancel := context.WithCancel(parent)

	stop := context.AfterFunc(doneCtx(done), func() {
		cancel()
	})

	return ctx, func() {
		stop()
		cancel()
	}
}
