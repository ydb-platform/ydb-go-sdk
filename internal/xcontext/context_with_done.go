package xcontext

import (
	"context"
)

type (
	withDoneOpts struct {
		onStart func()
		onDone  func()
	}
	withDoneOpt func(t *withDoneOpts)
)

func withDone(
	parent context.Context,
	done <-chan struct{},
	opts ...withDoneOpt,
) (context.Context, context.CancelFunc) {
	cfg := &withDoneOpts{}
	for _, opt := range opts {
		opt(cfg)
	}
	ctx, cancel := context.WithCancel(parent)
	go func() {
		if cfg.onStart != nil {
			cfg.onStart()
		}
		defer func() {
			if cfg.onDone != nil {
				cfg.onDone()
			}
		}()

		select {
		case <-ctx.Done():
		case <-done:
		}

		cancel()
	}()

	return ctx, cancel
}

func WithDone(parent context.Context, done <-chan struct{}) (context.Context, context.CancelFunc) {
	return withDone(parent, done)
}
