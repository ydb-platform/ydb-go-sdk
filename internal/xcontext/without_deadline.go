package xcontext

import (
	"context"
	"time"
)

type valueOnlyContext struct{ context.Context }

func (valueOnlyContext) Deadline() (time.Time, bool) { return time.Time{}, false }

func (valueOnlyContext) Done() <-chan struct{} { return nil }

func (valueOnlyContext) Err() error { return nil }

// WithoutDeadline helps to clear derived deadline from deadline
func WithoutDeadline(ctx context.Context) context.Context {
	return valueOnlyContext{ctx}
}
