package topicreadercommon

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetricDeltaQueueSerializesConcurrentObservers(t *testing.T) {
	var queue MetricDeltaQueue
	var stateMu sync.Mutex
	var workers sync.WaitGroup
	var observations []int
	next := 0
	emit := func(_ string, delta int) { observations = append(observations, delta) }
	const count = 100
	for range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for range count {
				stateMu.Lock()
				next++
				queue.Enqueue("topic", next)
				stateMu.Unlock()
				queue.Emit(emit)
			}
		}()
	}
	workers.Wait()
	require.Len(t, observations, 8*count)
	for i, value := range observations {
		require.Equal(t, i+1, value)
	}
}

func TestMetricDeltaQueueCanFinalizeAfterObserverPanic(t *testing.T) {
	var queue MetricDeltaQueue
	queue.Enqueue("topic", 1)
	require.Panics(t, func() {
		queue.Emit(func(string, int) { panic("observer failed") })
	})
	queue.Enqueue("topic", -1)
	var deltas []int
	queue.Emit(func(_ string, delta int) { deltas = append(deltas, delta) })
	require.Equal(t, []int{-1}, deltas)
}

func TestMetricDeltaQueueCompactsConsumedPrefixDuringReentrantEmission(t *testing.T) {
	const (
		initialBacklog = 4
		totalEvents    = 10_000
		maxCapacity    = 1_024
	)

	var queue MetricDeltaQueue
	for delta := range initialBacklog {
		queue.Enqueue("topic", delta+1)
	}

	observations := make([]int, 0, totalEvents)
	maxObservedCapacity := cap(queue.pending)
	nestedEmitAttempted := false
	queue.Emit(func(_ string, delta int) {
		observations = append(observations, delta)
		if !nestedEmitAttempted {
			nestedEmitAttempted = true
			queue.Emit(func(string, int) { require.FailNow(t, "nested emit callback must not run") })
		}
		if len(observations) <= totalEvents-initialBacklog {
			queue.Enqueue("topic", initialBacklog+len(observations))
		}
		maxObservedCapacity = max(maxObservedCapacity, cap(queue.pending))
	})

	require.Len(t, observations, totalEvents)
	for index, observed := range observations {
		require.Equal(t, index+1, observed)
	}
	require.LessOrEqual(t, maxObservedCapacity, maxCapacity)
}
