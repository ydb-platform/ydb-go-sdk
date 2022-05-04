package repeater

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
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

	name  string
	trace trace.Driver

	// Task is a function that must be executed periodically.
	task func(context.Context) error

	stop    context.CancelFunc
	stopped chan struct{}

	force int32
}

type option func(r *repeater)

func WithName(name string) option {
	return func(r *repeater) {
		r.name = name
	}
}

func WithTrace(trace trace.Driver) option {
	return func(r *repeater) {
		r.trace = trace
	}
}

func WithInterval(interval time.Duration) option {
	return func(r *repeater) {
		r.interval = interval
	}
}

type event string

const (
	eventTick  = event("tick")
	eventForce = event("force")
)

// New creates and begins to execute task periodically.
func New(
	interval time.Duration,
	task func(ctx context.Context) (err error),
	opts ...option,
) *repeater {
	ctx, stop := context.WithCancel(context.Background())

	r := &repeater{
		interval: interval,
		task:     task,
		stop:     stop,
		stopped:  make(chan struct{}),
	}

	for _, o := range opts {
		o(r)
	}

	go r.worker(ctx, r.interval)

	return r
}

// Stop stops to execute its task.
func (r *repeater) Stop() {
	r.stop()
	<-r.stopped
}

func (r *repeater) Force() {
	atomic.AddInt32(&r.force, 1)
}

func (r *repeater) wakeUp(ctx context.Context, e event) (err error) {
	if err = ctx.Err(); err != nil {
		return err
	}

	onDone := trace.DriverOnRepeaterWakeUp(
		r.trace,
		&ctx,
		r.name,
		string(e),
	)

	defer func() {
		onDone(err)

		if err != nil {
			atomic.StoreInt32(&r.force, 1)
		} else {
			atomic.StoreInt32(&r.force, 0)
		}
	}()

	return r.task(ctx)
}

func (r *repeater) worker(ctx context.Context, interval time.Duration) {
	defer close(r.stopped)

	tick := time.NewTicker(interval)
	defer tick.Stop()

	force := time.NewTicker(time.Second) // minimal interval between force wakeup's (maybe configured?)
	defer force.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-tick.C:
			_ = r.wakeUp(ctx, eventTick)

		case <-force.C:
			if atomic.LoadInt32(&r.force) != 0 {
				_ = r.wakeUp(ctx, eventForce)
			}
		}
	}
}
