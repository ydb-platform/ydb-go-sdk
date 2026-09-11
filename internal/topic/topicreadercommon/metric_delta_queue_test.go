package topicreadercommon

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreditBalanceSerializesConcurrentObservers(t *testing.T) {
	var balance CreditBalance
	var workers sync.WaitGroup
	var callbacks atomic.Int32
	var callbacksInFlight atomic.Int32
	var maxCallbacksInFlight atomic.Int32
	const count = 100

	emit := func(_ int) {
		inFlight := callbacksInFlight.Add(1)
		for {
			maxInFlight := maxCallbacksInFlight.Load()
			if inFlight <= maxInFlight || maxCallbacksInFlight.CompareAndSwap(maxInFlight, inFlight) {
				break
			}
		}
		callbacks.Add(1)
		callbacksInFlight.Add(-1)
	}

	for range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for range count {
				balance.Change(1, emit)
			}
		}()
	}
	workers.Wait()

	require.Equal(t, int32(8*count), callbacks.Load())
	require.Equal(t, int32(1), maxCallbacksInFlight.Load())
}

func TestCreditBalanceCanRecoverAfterObserverPanic(t *testing.T) {
	var balance CreditBalance
	require.Panics(t, func() {
		balance.Change(1, func(int) { panic("observer failed") })
	})
	var deltas []int
	balance.Change(-1, func(delta int) { deltas = append(deltas, delta) })
	require.Equal(t, []int{-1}, deltas)
}

func TestCreditBalanceCompactsConsumedPrefixDuringReentrantEmission(t *testing.T) {
	const (
		initialBacklog = 4
		totalEvents    = 10_000
		maxCapacity    = 1_024
	)

	var balance CreditBalance
	balance.mu.Lock()
	for delta := range initialBacklog {
		balance.enqueueLocked("", delta+1)
	}
	emitOwner := balance.startLocked()
	balance.mu.Unlock()
	require.True(t, emitOwner)

	observations := make([]int, 0, totalEvents)
	nestedEmitAttempted := false
	maxObservedCapacity := cap(balance.pending)
	tryEmit := func(emit func(int)) {
		balance.mu.Lock()
		emitOwner := balance.startLocked()
		balance.mu.Unlock()
		if emitOwner {
			balance.emit(emit)
		}
	}
	var emit func(int)
	emit = func(delta int) {
		observations = append(observations, delta)
		if !nestedEmitAttempted {
			nestedEmitAttempted = true
			tryEmit(func(int) { require.FailNow(t, "nested emit callback must not run") })
		}
		if len(observations) <= totalEvents-initialBacklog {
			balance.Change(initialBacklog+len(observations), emit)
		}
		maxObservedCapacity = max(maxObservedCapacity, cap(balance.pending))
	}
	balance.emit(emit)

	require.Len(t, observations, totalEvents)
	for index, observed := range observations {
		require.Equal(t, index+1, observed)
	}
	require.LessOrEqual(t, maxObservedCapacity, maxCapacity)
}
