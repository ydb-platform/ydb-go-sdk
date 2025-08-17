package xtest

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
)

// FastClock returns fake clock with very fast time speed advanced until end of test
// the clock stops advance at end of test
func FastClock(t testing.TB) *clockwork.FakeClock {
	clock := clockwork.NewFakeClock()
	var needStop atomic.Bool
	clockStopped := make(chan struct{})

	go func() {
		defer close(clockStopped)

		for {
			if needStop.Load() {
				return
			}

			clock.Advance(time.Second)
			time.Sleep(time.Microsecond)
		}
	}()

	t.Cleanup(func() {
		needStop.Store(true)
		<-clockStopped
	})

	return clock
}
