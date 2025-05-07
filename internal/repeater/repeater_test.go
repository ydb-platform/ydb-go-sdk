package repeater

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

func TestRepeaterNoWakeUpsAfterStop(t *testing.T) {
	var (
		interval    = time.Millisecond
		wakeUpStart = make(chan struct{})
		wakeUpDone  = make(chan struct{})
	)
	fakeClock := clockwork.NewFakeClock()
	r := New(context.Background(), interval, func(ctx context.Context) (err error) {
		wakeUpStart <- struct{}{}
		<-wakeUpDone

		return nil
	}, WithClock(fakeClock))

	fakeClock.Advance(interval)
	<-wakeUpStart            // wait first wake up
	wakeUpDone <- struct{}{} // unlock exit from first wake up

	fakeClock.Advance(interval)
	<-wakeUpStart   // wait second wake up
	r.stop(func() { // call stop
		wakeUpDone <- struct{}{} // unlock exit from second wake up
	})

	noWakeup := make(chan struct{})
	go func() {
		<-fakeClock.After(interval * 2)
		noWakeup <- struct{}{}
	}()

waitNoWakeup:
	for {
		fakeClock.Advance(interval)
		select {
		case <-wakeUpStart:
			t.Fatalf("unexpected wake up after stop")
		case <-noWakeup:
			break waitNoWakeup
		default:
			runtime.Gosched()
		}
	}
}

func TestRepeaterForceLogBackoff(t *testing.T) {
	delays := []time.Duration{
		0 * time.Second,
		1 * time.Second,  // 1 sec
		2 * time.Second,  // 3 sec
		4 * time.Second,  // 7 sec
		8 * time.Second,  // 15 sec
		16 * time.Second, // 31 sec
		32 * time.Second, // 63 sec
	}

	fakeClock := clockwork.NewFakeClock()
	var (
		wakeUps    = 0
		lastWakeUp = fakeClock.Now()
	)

	repeaterDone := make(chan struct{})
	r := New(context.Background(), 10*time.Minute, func(ctx context.Context) (err error) {
		defer func() {
			repeaterDone <- struct{}{}
		}()

		sinceLastWakeUp := fakeClock.Since(lastWakeUp)
		d := delays[wakeUps]
		if sinceLastWakeUp != d {
			t.Fatalf("unexpected wake up delay: %v, exp: %v", sinceLastWakeUp, d)
		}
		lastWakeUp = fakeClock.Now()
		wakeUps++

		return fmt.Errorf("special error for force with log backoff")
	}, WithClock(fakeClock))
	defer r.Stop()

	r.Force()
	<-repeaterDone

	for _, delay := range delays[1:] {
		// ensure right listeners attached
		err := fakeClock.BlockUntilContext(context.Background(), 2)
		require.NoError(t, err)

		// release trash timer listeners
		fakeClock.Advance(delay - 1)

		// fire estimated time
		fakeClock.Advance(1)
		<-repeaterDone
	}
}
