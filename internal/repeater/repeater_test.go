package repeater

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
)

func TestRepeaterNoWakeUpsAfterStop(t *testing.T) {
	testFunc := func(i int) {
		if i%100 == 0 {
			t.Logf("check %d ...", i)
			defer t.Logf("check %d done", i)
		}
		var (
			interval    = time.Millisecond
			wakeUpStart = make(chan struct{})
			wakeUpDone  = make(chan struct{})
		)
		fakeClock := clockwork.NewFakeClock()
		r := New(interval, func(ctx context.Context) (err error) {
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
			select {
			case <-wakeUpStart:
				t.Fatalf("unexpected wake up after stop")
			case <-fakeClock.After(interval * 2):
				noWakeup <- struct{}{}
			}
		}()

	waitNoWakeup:
		for {
			fakeClock.Advance(interval)
			select {
			case <-noWakeup:
				break waitNoWakeup
			default:
				runtime.Gosched()
			}
		}
	}
	for i := 0; i < 10000; i++ {
		testFunc(i)
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
		29 * time.Second, // 60 sec - normal ticker
		32 * time.Second, // 92 sec - force
	}

	fakeClock := clockwork.NewFakeClock()
	var (
		wakeUps    = 0
		lastWakeUp = fakeClock.Now()
	)

	repeaterDone := make(chan struct{})
	r := New(time.Minute, func(ctx context.Context) (err error) {
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
		return fmt.Errorf("spatial error for force with log backoff")
	}, WithClock(fakeClock))
	defer r.Stop()

	r.Force()
	<-repeaterDone

	for _, delay := range delays[1:] {
		fakeClock.Advance(delay - 2) // release trash timer listeners
		fakeClock.BlockUntil(2)      // ensure right listeners attached
		fakeClock.Advance(1)         // check about new listeners not fire before estimated
		fakeClock.Advance(1)         // fire estimated time
		<-repeaterDone
	}
}
