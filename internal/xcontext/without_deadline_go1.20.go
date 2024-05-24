//go:build !go1.21
// +build !go1.21

package xcontext

import (
	"context"
	"time"
)

type withoutCancelCtx struct {
	context.Context
}

func (withoutCancelCtx) Deadline() (deadline time.Time, ok bool) {
	return
}

func (withoutCancelCtx) Done() <-chan struct{} {
	return nil
}

func (withoutCancelCtx) Err() error {
	return nil
}

// ValueOnly helps to clear parent context from deadlines/cancels
func ValueOnly(parent context.Context) context.Context {
	return withoutCancelCtx{parent}
}
