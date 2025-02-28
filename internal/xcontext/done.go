package xcontext

import (
	"context"
)

func WithDone(parent context.Context, done <-chan struct{}) (context.Context, context.CancelFunc) {
	ctx, cancel := WithCancel(parent)
	select {
	case <-done:
		cancel()

		return ctx, cancel
	default:
	}

	go func() {
		select {
		case <-done:
			cancel()
		case <-parent.Done():
		}
	}()

	return ctx, cancel
}
