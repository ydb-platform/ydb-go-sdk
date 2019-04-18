package ydb

import (
	"context"
	"sync"
	"time"

	"github.com/yandex-cloud/ydb-go-sdk/timeutil"
)

// repeater contains logic of repeating some task.
type repeater struct {
	// Interval contains an interval between task execution.
	// Interval must be greater than zero; if not, Repeater will panic.
	Interval time.Duration

	// Timeout is an optional timeout for an operation passed as a context
	// instance.
	Timeout time.Duration

	// Task is a function that must be executed periodically.
	Task func(context.Context)

	timer     timeutil.Timer
	startOnce sync.Once
	stopOnce  sync.Once
	stop      chan struct{}
	done      chan struct{}
	ctx       context.Context
	cancel    context.CancelFunc
}

// Start begins to execute its task periodically.
func (r *repeater) Start() {
	r.startOnce.Do(func() {
		if r.Interval <= 0 {
			panic("repeater: non-positive interval")
		}
		r.timer = timeutil.NewTimer(r.Interval)
		r.stop = make(chan struct{})
		r.done = make(chan struct{})
		r.ctx, r.cancel = context.WithCancel(context.Background())
		go r.worker()
	})
}

// Stop stops to execute its task.
func (r *repeater) Stop() {
	var dummy bool
	r.startOnce.Do(func() {
		dummy = true
	})
	if dummy {
		return
	}
	r.stopOnce.Do(func() {
		close(r.stop)
		r.cancel()
		<-r.done
	})
}

func (r *repeater) worker() {
	defer close(r.done)
	for {
		select {
		case <-r.timer.C():
			r.timer.Reset(r.Interval)
		case <-r.stop:
			return
		}
		ctx := r.ctx
		if t := r.Timeout; t > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, t)
			defer cancel()
		}
		r.Task(ctx)
	}
}
