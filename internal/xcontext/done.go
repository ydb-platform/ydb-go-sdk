package xcontext

import (
	"context"
	"time"
)

type Done <-chan struct{}

func (done Done) Deadline() (deadline time.Time, ok bool) {
	return
}

func (done Done) Done() <-chan struct{} {
	return done
}

func (done Done) Err() error {
	select {
	case <-done:
		return context.Canceled
	default:
		return nil
	}
}

func (done Done) Value(key any) any {
	return nil
}

func WithDone(parent context.Context, done <-chan struct{}) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)

	select {
	case <-done:
		cancel()

		return ctx, cancel
	default:
	}

	stop := context.AfterFunc(Done(done), func() {
		cancel()
	})

	return ctx, func() {
		stop()
		cancel()
	}
}
