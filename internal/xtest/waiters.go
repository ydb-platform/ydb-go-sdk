package xtest

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
)

func WaitChannelClosed(t testing.TB, ch empty.Chan) {
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
func SpinWaitCondition(t testing.TB, l sync.Locker, cond func() bool) {
	t.Helper()
	SpinWaitConditionWithTimeout(t, l, time.Second, cond)
}

// SpinWaitConditionWithTimeout wait while cond return true with check it in loop
// l can be nil - then locker use for check conditions
func SpinWaitConditionWithTimeout(t testing.TB, l sync.Locker, condWaitTimeout time.Duration, cond func() bool) {
	t.Helper()

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
			t.Fatal()
		}

		runtime.Gosched()
	}
}
