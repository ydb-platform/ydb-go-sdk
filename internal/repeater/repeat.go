package repeater

import (
	"context"
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

	stop  chan struct{}
	done  chan struct{}
	force chan struct{}
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
	ctx context.Context,
	task func(ctx context.Context) (err error),
	opts ...option,
) Repeater {
	r := &repeater{
		task:  task,
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
		force: make(chan struct{}),
	}

	for _, o := range opts {
		o(r)
	}

	if r.interval <= 0 {
		return nil
	}

	go r.worker(ctx, r.interval)

	return r
}

// Stop stops to execute its task.
func (r *repeater) Stop() {
	close(r.stop)
}

func (r *repeater) Force() {
	select {
	case r.force <- struct{}{}:
	default:
	}
}

func (r *repeater) wakeUp(ctx context.Context, e event) {
	var (
		onDone = trace.DriverOnRepeaterWakeUp(
			r.trace,
			&ctx,
			r.name,
			string(e),
		)
		cancel context.CancelFunc
		err    error
	)

	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	err = r.task(ctx)

	onDone(err)

	if err != nil {
		r.Force()
	}
}

func (r *repeater) worker(ctx context.Context, interval time.Duration) {
	defer close(r.done)

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stop:
			return
		case <-time.After(interval):
			r.wakeUp(ctx, eventTick)
		case <-r.force:
			r.wakeUp(ctx, eventForce)
		}
	}
}
