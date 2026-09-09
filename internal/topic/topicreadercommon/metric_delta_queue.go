package topicreadercommon

import "sync"

const metricDeltaQueueCompactionThreshold = 256

// MetricDeltaQueue preserves balance-event order across concurrent and reentrant
// trace callbacks. Enqueue while holding the balance lock; Emit after releasing it.
// No user callback runs under either mutex.
type MetricDeltaQueue struct {
	mu       sync.Mutex
	pending  []metricDelta
	head     int
	emitting bool
}

type metricDelta struct {
	topic string
	delta int
}

func (q *MetricDeltaQueue) Enqueue(topic string, delta int) {
	if delta == 0 {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pending = append(q.pending, metricDelta{topic: topic, delta: delta})
}

func (q *MetricDeltaQueue) Emit(emit func(string, int)) {
	if !q.start() {
		return
	}
	defer func() {
		if p := recover(); p != nil {
			q.stop()
			panic(p)
		}
	}()
	for {
		event, ok := q.next()
		if !ok {
			return
		}
		emit(event.topic, event.delta)
	}
}

func (q *MetricDeltaQueue) start() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.emitting || len(q.pending) == 0 {
		return false
	}
	q.emitting = true

	return true
}

func (q *MetricDeltaQueue) stop() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.emitting = false
}

func (q *MetricDeltaQueue) next() (metricDelta, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.head == len(q.pending) {
		q.pending = q.pending[:0]
		q.head = 0
		q.emitting = false

		return metricDelta{}, false
	}
	event := q.pending[q.head]
	q.pending[q.head] = metricDelta{}
	q.head++
	if q.head >= metricDeltaQueueCompactionThreshold && q.head >= len(q.pending)-q.head {
		newLength := len(q.pending) - q.head
		copy(q.pending, q.pending[q.head:])
		clear(q.pending[newLength:])
		q.pending = q.pending[:newLength]
		q.head = 0
	}

	return event, true
}
