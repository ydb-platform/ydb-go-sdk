package table

import (
	"context"
)

// Operation is the interface that holds an operation for retry.
type Operation func(context.Context, Session) (err error)

type Option func(o *Options)

type Options struct {
	Idempotent bool
}

func WithIdempotent() Option {
	return func(o *Options) {
		o.Idempotent = true
	}
}

type Client interface {
	// Close closes table client
	Close(ctx context.Context) error

	// Do provide the best effort for execute operation
	// Do implements internal busy loop until one of the following conditions is met:
	// - deadline was canceled or deadlined
	// - retry operation returned nil as error
	// Warning: if deadline without deadline or cancellation func Retry will be worked infinite
	Do(ctx context.Context, op Operation, opts ...Option) (err error)
}
