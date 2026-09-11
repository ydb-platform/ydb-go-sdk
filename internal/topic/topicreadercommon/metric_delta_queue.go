package topicreadercommon

import "sync"

const metricDeltaQueueCompactionThreshold = 256

type metricDelta struct {
	topic string
	delta int
}

// CreditBalance owns a signed credit balance and its pending trace events.
// The balance and event state intentionally share one mutex so event order is
// established by the same critical section that changes the balance.
type CreditBalance struct {
	metricDeltaState

	mu        sync.Mutex
	balance   int64
	finalized bool
}

// Change applies delta and invokes emit after releasing the balance mutex. A
// nil callback leaves the owner untouched, which keeps disabled tracing cheap.
func (b *CreditBalance) Change(delta int, emit func(int)) {
	if delta == 0 || emit == nil {
		return
	}
	if !b.change(delta) {
		return
	}
	b.emit(emit)
}

func (b *CreditBalance) change(delta int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.finalized {
		return false
	}
	b.balance += int64(delta)
	b.enqueueLocked("", delta)

	return b.startLocked()
}

// Finalize emits the inverse of the outstanding balance once. Changes after
// finalization and repeated finalization calls are ignored.
func (b *CreditBalance) Finalize(emit func(int)) {
	if emit == nil {
		return
	}
	if !b.finalize() {
		return
	}
	b.emit(emit)
}

func (b *CreditBalance) finalize() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.finalized {
		return false
	}
	balance := b.balance
	b.balance = 0
	b.finalized = true
	if balance != 0 {
		b.enqueueLocked("", -int(balance))
	}

	return b.startLocked()
}

func (b *CreditBalance) emit(emit func(int)) {
	b.metricDeltaState.emit(&b.mu, func(event metricDelta) {
		emit(event.delta)
	})
}

// LocalBufferBalance owns per-topic buffered-message counts and their pending
// trace events. The map and event state share one mutex for atomic accounting.
type LocalBufferBalance struct {
	metricDeltaState

	mu        sync.Mutex
	byTopic   map[string]int64
	finalized bool
}

// Reserve adds messages for topic and invokes emit after releasing the mutex.
// It reports whether the reservation was accepted.
func (b *LocalBufferBalance) Reserve(topic string, messagesCount int, emit func(string, int)) bool {
	if messagesCount <= 0 || emit == nil {
		return false
	}
	accepted, emitOwner := b.reserve(topic, messagesCount)
	if !accepted {
		return false
	}
	if emitOwner {
		b.emit(emit)
	}

	return true
}

func (b *LocalBufferBalance) reserve(topic string, messagesCount int) (bool, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.finalized {
		return false, false
	}
	if b.byTopic == nil {
		b.byTopic = make(map[string]int64)
	}
	b.byTopic[topic] += int64(messagesCount)
	b.enqueueLocked(topic, messagesCount)

	return true, b.startLocked()
}

// Release removes messages from topic when the tracked balance permits it.
// Invalid, late, and over-release attempts are ignored.
func (b *LocalBufferBalance) Release(topic string, messagesCount int, emit func(string, int)) {
	if messagesCount <= 0 || emit == nil {
		return
	}
	if !b.release(topic, messagesCount) {
		return
	}
	b.emit(emit)
}

func (b *LocalBufferBalance) release(topic string, messagesCount int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.finalized || b.byTopic == nil {
		return false
	}
	balance, ok := b.byTopic[topic]
	if !ok || balance < int64(messagesCount) {
		return false
	}
	if balance == int64(messagesCount) {
		delete(b.byTopic, topic)
	} else {
		b.byTopic[topic] = balance - int64(messagesCount)
	}
	b.enqueueLocked(topic, -messagesCount)

	return b.startLocked()
}

// Finalize emits inverse deltas for every outstanding topic balance once.
func (b *LocalBufferBalance) Finalize(emit func(string, int)) {
	if emit == nil {
		return
	}
	if !b.finalize() {
		return
	}
	b.emit(emit)
}

func (b *LocalBufferBalance) finalize() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.finalized {
		return false
	}
	for topic, balance := range b.byTopic {
		if balance != 0 {
			b.enqueueLocked(topic, -int(balance))
		}
	}
	b.byTopic = nil
	b.finalized = true

	return b.startLocked()
}

func (b *LocalBufferBalance) emit(emit func(string, int)) {
	b.metricDeltaState.emit(&b.mu, func(event metricDelta) {
		emit(event.topic, event.delta)
	})
}

type metricDeltaState struct {
	pending  []metricDelta
	head     int
	emitting bool
}

func (s *metricDeltaState) enqueueLocked(topic string, delta int) {
	s.pending = append(s.pending, metricDelta{topic: topic, delta: delta})
}

func (s *metricDeltaState) startLocked() bool {
	if s.emitting || len(s.pending) == 0 {
		return false
	}
	s.emitting = true

	return true
}

func (s *metricDeltaState) next(mu *sync.Mutex) (metricDelta, bool) {
	mu.Lock()
	defer mu.Unlock()

	if s.head == len(s.pending) {
		s.pending = s.pending[:0]
		s.head = 0
		s.emitting = false

		return metricDelta{}, false
	}
	event := s.pending[s.head]
	s.pending[s.head] = metricDelta{}
	s.head++
	if s.head >= metricDeltaQueueCompactionThreshold && s.head >= len(s.pending)-s.head {
		newLength := len(s.pending) - s.head
		copy(s.pending, s.pending[s.head:])
		clear(s.pending[newLength:])
		s.pending = s.pending[:newLength]
		s.head = 0
	}

	return event, true
}

func (s *metricDeltaState) stop(mu *sync.Mutex) {
	mu.Lock()
	defer mu.Unlock()
	s.emitting = false
}

func (s *metricDeltaState) emit(mu *sync.Mutex, emit func(metricDelta)) {
	defer func() {
		if p := recover(); p != nil {
			s.stop(mu)
			panic(p)
		}
	}()
	for {
		event, ok := s.next(mu)
		if !ok {
			return
		}
		emit(event)
	}
}
