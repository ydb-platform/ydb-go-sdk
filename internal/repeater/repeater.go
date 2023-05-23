package repeater

import (
	"context"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
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
	trace *trace.Driver

	// Task is a function that must be executed periodically.
	task func(context.Context) error

	cancel  context.CancelFunc
	stopped chan struct{}

	force chan struct{}
	clock clockwork.Clock
}

type option func(r *repeater)

func WithName(name string) option {
	return func(r *repeater) {
		r.name = name
	}
}

func WithTrace(trace *trace.Driver) option {
	return func(r *repeater) {
		r.trace = trace
	}
}

func WithInterval(interval time.Duration) option {
	return func(r *repeater) {
		r.interval = interval
	}
}

func WithClock(clock clockwork.Clock) option {
	return func(r *repeater) {
		r.clock = clock
	}
}

type event string

const (
	eventTick   = event("tick")
	eventForce  = event("force")
	eventCancel = event("cancel")
)

// New creates and begins to execute task periodically.
func New(
	interval time.Duration,
	task func(ctx context.Context) (err error),
	opts ...option,
) *repeater {
	ctx, cancel := xcontext.WithCancel(context.Background())

	r := &repeater{
		interval: interval,
		task:     task,
		cancel:   cancel,
		stopped:  make(chan struct{}),
		force:    make(chan struct{}, 1),
		clock:    clockwork.NewRealClock(),
		trace:    &trace.Driver{},
	}

	for _, o := range opts {
		if o != nil {
			o(r)
		}
	}

	go r.worker(ctx, r.clock.NewTicker(interval))

	return r
}

func (r *repeater) stop(onCancel func()) {
	r.cancel()
	if onCancel != nil {
		onCancel()
	}
	<-r.stopped
}

// Stop stops to execute its task.
func (r *repeater) Stop() {
	r.stop(nil)
}

func (r *repeater) Force() {
	select {
	case r.force <- struct{}{}:
	default:
	}
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
			r.Force()
		} else {
			select {
			case <-r.force:
			default:
			}
		}
	}()

	return r.task(ctx)
}

func (r *repeater) worker(ctx context.Context, tick clockwork.Ticker) {
	defer close(r.stopped)
	defer tick.Stop()

	// force returns backoff with delays [500ms...32s]
	force := backoff.New(
		backoff.WithSlotDuration(500*time.Millisecond),
		backoff.WithCeiling(6),
		backoff.WithJitterLimit(1),
		backoff.WithClock(r.clock),
	)

	// forceIndex defines delay index for force backoff
	forceIndex := 0

	waitForceEvent := func() event {
		if forceIndex == 0 {
			return eventForce
		}
		select {
		case <-ctx.Done():
			return eventCancel
		case <-tick.Chan():
			return eventTick
		case <-force.Wait(forceIndex):
			return eventForce
		}
	}

	// processEvent func checks wakeup error and returns new force index
	processEvent := func(event event) {
		if event == eventCancel {
			return
		}
		if err := r.wakeUp(ctx, event); err != nil {
			forceIndex++
		} else {
			forceIndex = 0
		}
	}

	for {
		select {
		case <-ctx.Done():
			return

		case <-tick.Chan():
			processEvent(eventTick)

		case <-r.force:
			processEvent(waitForceEvent())
		}
	}
}
