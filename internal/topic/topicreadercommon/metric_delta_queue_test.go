package topicreadercommon

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreditBalanceSerializesConcurrentObservers(t *testing.T) {
	var balance MetricBalance
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

func TestCreditBalanceCloseCompensatesConcurrentChanges(t *testing.T) {
	var balance MetricBalance
	var total atomic.Int64
	emit := func(delta int) { total.Add(int64(delta)) }
	balance.Change(1, emit)
	var workers sync.WaitGroup
	start := make(chan struct{})
	for range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			balance.Change(1, emit)
		}()
	}
	close(start)
	balance.Finalize(emit)
	workers.Wait()
	balance.Finalize(emit)
	require.Zero(t, total.Load())
}

func TestCreditBalanceCanRecoverAfterObserverPanic(t *testing.T) {
	var balance MetricBalance
	require.Panics(t, func() {
		balance.Change(1, func(int) { panic("observer failed") })
	})
	var deltas []int
	balance.Change(-1, func(delta int) { deltas = append(deltas, delta) })
	require.Equal(t, []int{-1}, deltas)
}

func TestBufferBalanceTracksAndFinalizesReaderDeltas(t *testing.T) {
	var events []int
	emit := func(delta int) {
		events = append(events, delta)
	}
	var balance MetricBalance
	balance.Finalize(nil)
	balance.ChangeBuffer(-1, emit)
	require.False(t, balance.ChangeBuffer(0, emit))
	require.False(t, balance.ChangeBuffer(1, nil))
	var empty MetricBalance
	empty.Finalize(emit)
	empty.Finalize(emit)
	require.False(t, empty.ChangeBuffer(1, emit))
	empty.ChangeBuffer(-1, emit)
	require.Empty(t, events)

	require.True(t, balance.ChangeBuffer(3, emit))
	balance.ChangeBuffer(-3, nil)
	balance.ChangeBuffer(0, emit)
	require.True(t, balance.ChangeBuffer(2, emit))
	balance.ChangeBuffer(-6, emit)
	balance.ChangeBuffer(-7, emit)
	balance.ChangeBuffer(-1, emit)
	balance.ChangeBuffer(-2, emit)
	require.Equal(t, []int{
		3,
		2,
		-1,
		-2,
	}, events)

	balance.Finalize(emit)
	balance.Finalize(emit)
	require.False(t, balance.ChangeBuffer(1, emit))
	balance.ChangeBuffer(-1, emit)
	require.Equal(t, []int{
		3,
		2,
		-1,
		-2,
		-2,
	}, events)

	var zeroBalance MetricBalance
	require.True(t, zeroBalance.ChangeBuffer(1, emit))
	zeroBalance.ChangeBuffer(-1, emit)
	zeroBalance.Finalize(emit)
	zeroBalance.Finalize(emit)
	require.Equal(t, []int{
		3,
		2,
		-1,
		-2,
		-2,
		1,
		-1,
	}, events)
}

func TestCreditBalanceCompactsConsumedPrefixDuringReentrantEmission(t *testing.T) {
	const (
		initialBacklog = 300
		totalEvents    = 10_000
		maxCapacity    = 1_024
	)

	var balance MetricBalance
	observations := make([]int, 0, totalEvents)
	nestedCallbackCalled := false
	maxObservedCapacity := cap(balance.pending)
	var emit func(int)
	emit = func(delta int) {
		observations = append(observations, delta)
		if len(observations) == 1 {
			balance.Change(2, func(int) { nestedCallbackCalled = true })
			for delta := 3; delta <= initialBacklog+1; delta++ {
				balance.Change(delta, emit)
			}
		} else if len(observations) <= totalEvents-initialBacklog {
			balance.Change(initialBacklog+len(observations), emit)
		}
		maxObservedCapacity = max(maxObservedCapacity, cap(balance.pending))
	}
	balance.Change(1, emit)

	require.Len(t, observations, totalEvents)
	for index, observed := range observations {
		require.Equal(t, index+1, observed)
	}
	require.False(t, nestedCallbackCalled)
	require.LessOrEqual(t, maxObservedCapacity, maxCapacity)
}
