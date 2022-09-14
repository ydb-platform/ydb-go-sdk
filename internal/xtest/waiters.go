package xtest

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

func WaitChannelClosed(t testing.TB, ch <-chan struct{}) {
	t.Helper()

	const condWaitTimeout = time.Second

	select {
	case <-time.After(condWaitTimeout):
		t.Fatal()
	case <-ch:
		// pass
	}
}

// SpinWaitCondition wait while cond return true with check it in loop
// l can be nil - then locker use for check conditions
func SpinWaitCondition(tb testing.TB, l sync.Locker, cond func() bool) {
	tb.Helper()
	SpinWaitConditionWithTimeout(tb, l, time.Second, cond)
}

// SpinWaitConditionWithTimeout wait while cond return true with check it in loop
// l can be nil - then locker use for check conditions
func SpinWaitConditionWithTimeout(tb testing.TB, l sync.Locker, condWaitTimeout time.Duration, cond func() bool) {
	tb.Helper()

	checkConditin := func() bool {
		if l != nil {
			l.Lock()
			defer l.Unlock()
		}
		return cond()
	}

	start := time.Now()
	for {
		if checkConditin() {
			return
		}

		if time.Since(start) > condWaitTimeout {
			tb.Fatal()
		}

		runtime.Gosched()
	}
}
