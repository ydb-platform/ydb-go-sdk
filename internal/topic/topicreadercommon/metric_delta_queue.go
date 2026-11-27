package topicreadercommon

import "sync"

// MetricBalance compensates the outstanding signed balance exactly once on close.
// Only overlapping callbacks need a backlog: normal changes emit directly, without allocation.
type MetricBalance struct {
	mu       sync.Mutex
	value    int
	closed   bool
	emitting bool
	pending  []int
}

type balanceChange uint8

const (
	creditChange balanceChange = iota
	bufferChange
	closeBalance
)

func (b *MetricBalance) Change(delta int, emit func(int)) { b.apply(creditChange, delta, emit) }

// ChangeBuffer rejects releases larger than the reader's outstanding reservation.
func (b *MetricBalance) ChangeBuffer(delta int, emit func(int)) bool {
	return b.apply(bufferChange, delta, emit)
}

func (b *MetricBalance) Finalize(emit func(int)) { b.apply(closeBalance, 0, emit) }

func (b *MetricBalance) apply(kind balanceChange, delta int, emit func(int)) bool {
	if emit == nil {
		return false
	}
	accepted, first := b.record(kind, delta)
	if first != 0 {
		b.drain(first, emit)
	}

	return accepted
}

func (b *MetricBalance) record(kind balanceChange, delta int) (bool, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return false, 0
	}
	switch kind {
	case closeBalance:
		b.closed = true
		delta = -b.value
	case bufferChange:
		if delta < -b.value {
			return false, 0
		}
	case creditChange:
		// Credit may go negative for an oversized response.
	}
	if delta == 0 {
		return false, 0
	}
	b.value += delta
	if b.emitting {
		b.pending = append(b.pending, delta)

		return true, 0
	}
	b.emitting = true
	if len(b.pending) != 0 {
		b.pending = append(b.pending, delta)
		delta = b.pending[0]
		b.pending = b.pending[1:]
	}

	return true, delta
}

// Drain outside the mutex: observers can synchronously finalize the same stream.
func (b *MetricBalance) drain(delta int, emit func(int)) {
	defer func() {
		if p := recover(); p != nil {
			b.mu.Lock()
			b.emitting = false
			b.mu.Unlock()
			panic(p)
		}
	}()
	for delta != 0 {
		emit(delta)
		delta = b.next()
	}
}

func (b *MetricBalance) next() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.pending) == 0 {
		b.emitting = false
		b.pending = nil

		return 0
	}
	delta := b.pending[0]
	b.pending = b.pending[1:]

	return delta
}
