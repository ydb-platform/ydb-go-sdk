package xtest

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
)

const commonWaitTimeout = time.Second

func WaitGroup(tb testing.TB, wg *sync.WaitGroup) {
	tb.Helper()

	WaitGroupWithTimeout(tb, wg, commonWaitTimeout)
}

func WaitGroupWithTimeout(tb testing.TB, wg *sync.WaitGroup, timeout time.Duration) {
	tb.Helper()

	groupFinished := make(empty.Chan)
	go func() {
		wg.Wait()
		close(groupFinished)
	}()

	WaitChannelClosedWithTimeout(tb, groupFinished, timeout)
}

func WaitChannelClosed(t testing.TB, ch <-chan struct{}) {
	t.Helper()

	WaitChannelClosedWithTimeout(t, ch, commonWaitTimeout)
}

func WaitChannelClosedWithTimeout(t testing.TB, ch <-chan struct{}, timeout time.Duration) {
	t.Helper()

	select {
	case <-time.After(timeout):
		t.Fatal("failed timeout")
	case <-ch:
		// pass
	}
}

// SpinWaitCondition wait while cond return true with check it in loop
// l can be nil - then locker use for check conditions
func SpinWaitCondition(tb testing.TB, l sync.Locker, cond func() bool) {
	tb.Helper()

	SpinWaitConditionWithTimeout(tb, l, commonWaitTimeout, cond)
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

// SpinWaitProgress failed if result of progress func no changes without timeout
func SpinWaitProgress(tb testing.TB, progress func() (progressValue interface{}, finished bool)) {
	tb.Helper()
	SpinWaitProgressWithTimeout(tb, commonWaitTimeout, progress)
}

func SpinWaitProgressWithTimeout(
	tb testing.TB,
	timeout time.Duration,
	progress func() (progressValue interface{}, finished bool),
) {
	tb.Helper()

	for {
		currentValue, finished := progress()
		if finished {
			return
		}

		SpinWaitConditionWithTimeout(tb, nil, timeout, func() bool {
			progressValue, finished := progress()

			return finished || progressValue != currentValue
		})
	}
}
