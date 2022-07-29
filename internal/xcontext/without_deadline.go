package xcontext

import (
	"context"
	"time"
)

type valueOnlyContext struct{ context.Context }

func (valueOnlyContext) Deadline() (deadline time.Time, ok bool) { return }

func (valueOnlyContext) Done() <-chan struct{} { return nil }

func (valueOnlyContext) Err() error { return nil }

// WithoutDeadline helps to clear derived deadline from deadline
func WithoutDeadline(ctx context.Context) context.Context {
	return valueOnlyContext{ctx}
}
