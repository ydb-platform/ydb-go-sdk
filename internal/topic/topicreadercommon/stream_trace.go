package topicreadercommon

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type StreamTraceQueue struct {
	work         *xsync.UnboundedChan[func()]
	mu           sync.Mutex
	closed       bool
	observesACKs bool
	// ackQueued tells the receive loop that the current response queued an ACK.
	ackQueued atomic.Bool
	// One reusable marker per stream keeps the barrier allocation-free.
	ackDone    chan struct{}
	signalACKs func()
}

func (q *StreamTraceQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.work.Close()
}

// WaitForACKs bounds queued ACKs to one wire response. An observer may call
// Close, so the receive loop must stop waiting when shutdown joins it.
// A response that queued no ACK does not wait.
func (q *StreamTraceQueue) WaitForACKs(stop <-chan struct{}) bool {
	if !q.observesACKs || !q.ackQueued.Swap(false) {
		return true
	}
	q.work.Send(q.signalACKs)
	select {
	case <-q.ackDone:
		return true
	case <-stop:
		return false
	}
}

// Each hook owns its keys. Queue a callback only on the first update to a key;
// later updates accumulate there until the consumer takes it, even across other keys.
func coalesceTrace[K comparable, V any](q *StreamTraceQueue, observe func(V), sum func(V, V) V) func(K, V) {
	pending := make(map[K]V)

	return func(key K, latest V) {
		q.mu.Lock()
		defer q.mu.Unlock()
		if q.closed {
			return
		}
		if previous, ok := pending[key]; ok {
			pending[key] = sum(latest, previous)

			return
		}
		pending[key] = latest
		q.work.Send(func() {
			q.mu.Lock()
			event := pending[key]
			delete(pending, key)
			q.mu.Unlock()
			observe(event)
		})
	}
}

type streamHooks struct {
	received *func(trace.TopicReaderMessagesReceivedInfo)
	credit   *func(trace.TopicReaderCreditBalanceChangedInfo)
	local    *func(trace.TopicReaderLocalBufferChangedInfo)
}

func workerHooks(t *trace.Topic, listener bool) streamHooks {
	if listener {
		return streamHooks{
			received: &t.OnListenerMessagesReceived,
			credit:   &t.OnListenerCreditBalanceChanged,
			local:    &t.OnListenerLocalBufferChanged,
		}
	}

	return streamHooks{
		received: &t.OnReaderMessagesReceived,
		credit:   &t.OnReaderCreditBalanceChanged,
		local:    &t.OnReaderLocalBufferChanged,
	}
}

// StreamTrace installs a single consumer for coalesced worker hooks of this stream.
// Closing the queue never joins the consumer: an observer may itself be calling Close.
func StreamTrace(t *trace.Topic, info ReaderInfo, onPanic func(error)) (*trace.Topic, *StreamTraceQueue) {
	if t == nil {
		return nil, nil
	}
	h := workerHooks(t, info.Listener)
	if *h.credit == nil && *h.local == nil && *h.received == nil && t.OnReaderCommittedNotify == nil {
		return t, nil
	}
	q := &StreamTraceQueue{work: xsync.NewUnboundedChan[func()]()}
	wrapped := *t
	h = workerHooks(&wrapped, info.Listener)
	if fn := *h.credit; fn != nil {
		send := coalesceTrace[int](q, fn, func(
			a, b trace.TopicReaderCreditBalanceChangedInfo,
		) trace.TopicReaderCreditBalanceChangedInfo {
			a.BytesDelta += b.BytesDelta

			return a
		})
		*h.credit = func(i trace.TopicReaderCreditBalanceChangedInfo) { send(0, i) }
	}
	if fn := *h.local; fn != nil {
		send := coalesceTrace[int](q, fn, func(
			a, b trace.TopicReaderLocalBufferChangedInfo,
		) trace.TopicReaderLocalBufferChangedInfo {
			a.MessagesDelta += b.MessagesDelta

			return a
		})
		*h.local = func(i trace.TopicReaderLocalBufferChangedInfo) { send(0, i) }
	}
	if fn := *h.received; fn != nil {
		send := coalesceTrace[string](q, fn, func(
			a, b trace.TopicReaderMessagesReceivedInfo,
		) trace.TopicReaderMessagesReceivedInfo {
			a.MessagesCount += b.MessagesCount

			return a
		})
		*h.received = func(i trace.TopicReaderMessagesReceivedInfo) { send(i.Topic, i) }
	}
	if fn := t.OnReaderCommittedNotify; fn != nil {
		q.observesACKs = true
		q.ackDone = make(chan struct{}, 1)
		q.signalACKs = func() {
			// A marker that runs after shutdown left the wait must not block the consumer.
			select {
			case q.ackDone <- struct{}{}:
			default:
			}
		}
		wrapped.OnReaderCommittedNotify = func(i trace.TopicReaderCommittedNotifyInfo) {
			q.ackQueued.Store(true)
			q.work.Send(func() { fn(i) })
		}
	}
	go drainStreamTrace(q.work, onPanic)

	return &wrapped, q
}

func drainStreamTrace(q *xsync.UnboundedChan[func()], onPanic func(error)) {
	q.Range(func(observe func()) {
		// Retire once on the first panic, but keep delivering the rest of the FIFO,
		// including compensation. A broken observer must not terminate the application.
		defer func() {
			if p := recover(); p != nil && onPanic != nil {
				onPanic(fmt.Errorf("ydb: topic stream trace panic: %v", p))
				onPanic = nil
			}
		}()
		observe()
	})
}
