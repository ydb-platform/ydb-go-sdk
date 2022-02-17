package repeater

import (
	"context"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/testutil/timeutil"
)

type Repeater interface {
	Stop()
	Force()
}

// repeater contains logic of repeating some task.
type repeater struct {
	// Interval contains an interval between task execution.
	// Interval must be greater than zero; if not, Repeater will panic.
	interval time.Duration

	// Task is a function that must be executed periodically.
	task func(context.Context) error

	timer    timeutil.Timer
	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
	ctx      context.Context
	cancel   context.CancelFunc
}

type optionsHolder struct {
	runTaskOnInit bool
}

type option func(h *optionsHolder)

func WithRunTaskOnInit() option {
	return func(h *optionsHolder) {
		h.runTaskOnInit = true
	}
}

// NewRepeater creates and begins to execute task periodically.
func NewRepeater(
	ctx context.Context,
	interval time.Duration,
	task func(ctx context.Context) (err error),
	opts ...option,
) Repeater {
	if interval <= 0 {
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	r := &repeater{
		interval: interval,
		task:     task,
		timer:    timeutil.NewTimer(interval),
		stopOnce: sync.Once{},
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
		ctx:      ctx,
		cancel:   cancel,
	}
	go r.worker(opts...)
	return r
}

// Stop stops to execute its task.
func (r *repeater) Stop() {
	r.stopOnce.Do(func() {
		close(r.stop)
		r.cancel()
		<-r.done
	})
}

func (r *repeater) Force() {
	if !r.timer.Stop() {
		<-r.timer.C()
	}
	r.timer.Reset(0)
}

func (r *repeater) singleTask() {
	if err := r.task(r.ctx); err != nil {
		r.timer.Reset(time.Second)
	} else {
		r.timer.Reset(r.interval)
	}
}

func (r *repeater) worker(opts ...option) {
	defer func() {
		close(r.done)
	}()
	h := &optionsHolder{}
	for _, o := range opts {
		o(h)
	}
	if h.runTaskOnInit {
		r.singleTask()
	}
	for {
		select {
		case <-r.stop:
			return
		case <-r.timer.C():
			r.singleTask()
		}
	}
}
