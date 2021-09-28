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
	task func(context.Context)

	timer    timeutil.Timer
	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
	ctx      context.Context
	cancel   context.CancelFunc
	force    chan struct{}
}

// NewRepeater creates and begins to execute task periodically.
func NewRepeater(interval time.Duration, task func(ctx context.Context), onDone func()) Repeater {
	if interval <= 0 {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &repeater{
		interval: interval,
		task:     task,
		timer:    timeutil.NewTimer(interval),
		stopOnce: sync.Once{},
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
		ctx:      ctx,
		cancel:   cancel,
		force:    make(chan struct{}),
	}
	go r.worker(onDone)
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
	select {
	case r.force <- struct{}{}:
	default:
	}
}

func (r *repeater) worker(onDone func()) {
	defer func() {
		close(r.done)
		onDone()
	}()
	for {
		r.task(r.ctx)
		select {
		case <-r.stop:
			return
		case <-r.timer.C():

		case <-r.force:
			if !r.timer.Stop() {
				<-r.timer.C()
			}
		}
		r.timer.Reset(r.interval)
	}
}
