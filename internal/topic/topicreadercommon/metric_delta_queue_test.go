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

func TestLocalBufferBalanceTracksAndFinalizesTopicDeltas(t *testing.T) {
	var events []metricDelta
	emit := func(topic string, delta int) {
		events = append(events, metricDelta{topic: topic, delta: delta})
	}
	var balance LocalBufferBalance
	balance.Finalize(nil)
	balance.Release("missing", 1, emit)
	require.False(t, balance.Reserve("ignored", 0, emit))
	require.False(t, balance.Reserve("ignored", 1, nil))
	var empty LocalBufferBalance
	empty.Finalize(emit)
	empty.Finalize(emit)
	require.False(t, empty.Reserve("late", 1, emit))
	empty.Release("late", 1, emit)
	require.Empty(t, events)

	require.True(t, balance.Reserve("topic-a", 3, emit))
	balance.Release("topic-a", 3, nil)
	balance.Release("topic-a", 0, emit)
	require.True(t, balance.Reserve("topic-b", 2, emit))
	balance.Release("missing", 1, emit)
	balance.Release("topic-a", 4, emit)
	balance.Release("topic-a", 1, emit)
	balance.Release("topic-b", 2, emit)
	require.Equal(t, []metricDelta{
		{topic: "topic-a", delta: 3},
		{topic: "topic-b", delta: 2},
		{topic: "topic-a", delta: -1},
		{topic: "topic-b", delta: -2},
	}, events)

	balance.Finalize(emit)
	balance.Finalize(emit)
	require.False(t, balance.Reserve("topic-a", 1, emit))
	balance.Release("topic-a", 1, emit)
	require.Equal(t, []metricDelta{
		{topic: "topic-a", delta: 3},
		{topic: "topic-b", delta: 2},
		{topic: "topic-a", delta: -1},
		{topic: "topic-b", delta: -2},
		{topic: "topic-a", delta: -2},
	}, events)

	var zeroBalance LocalBufferBalance
	require.True(t, zeroBalance.Reserve("drained", 1, emit))
	zeroBalance.Release("drained", 1, emit)
	zeroBalance.Finalize(emit)
	zeroBalance.Finalize(emit)
	require.Equal(t, []metricDelta{
		{topic: "topic-a", delta: 3},
		{topic: "topic-b", delta: 2},
		{topic: "topic-a", delta: -1},
		{topic: "topic-b", delta: -2},
		{topic: "topic-a", delta: -2},
		{topic: "drained", delta: 1},
		{topic: "drained", delta: -1},
	}, events)
}

func TestCreditBalanceCompactsConsumedPrefixDuringReentrantEmission(t *testing.T) {
	const (
		initialBacklog = 300
		totalEvents    = 10_000
		maxCapacity    = 1_024
	)

	var balance CreditBalance
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
