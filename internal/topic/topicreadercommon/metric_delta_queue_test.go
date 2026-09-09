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
