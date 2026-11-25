package topicreadercommon

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// streamTraceEvent keeps frequent gauge and message events free of per-event closures.
type streamTraceEvent struct {
	run   func(streamTraceEvent)
	ctx   *context.Context
	topic string
	count int
}

// StreamTrace installs a single FIFO consumer for the worker hooks of this stream.
// Closing the queue never joins the consumer: an observer may itself be calling Close.
func StreamTrace(t *trace.Topic, info ReaderInfo, onPanic func(error)) (*trace.Topic, func()) {
	if t == nil || (t.OnReaderCreditBalanceChanged == nil && t.OnReaderLocalBufferChanged == nil &&
		t.OnReaderMessagesReceived == nil && t.OnReaderCommittedNotify == nil &&
		(!info.Listener || t.OnReaderMessagesDelivered == nil)) {
		return t, nil
	}
	q := xsync.NewUnboundedChan[streamTraceEvent]()
	wrapped := *t
	if fn := t.OnReaderCreditBalanceChanged; fn != nil {
		run := func(e streamTraceEvent) {
			fn(trace.TopicReaderCreditBalanceChangedInfo{Context: e.ctx, ReaderInfo: info, BytesDelta: e.count})
		}
		wrapped.OnReaderCreditBalanceChanged = func(i trace.TopicReaderCreditBalanceChangedInfo) {
			q.Send(streamTraceEvent{run: run, ctx: i.Context, count: i.BytesDelta})
		}
	}
	if fn := t.OnReaderLocalBufferChanged; fn != nil {
		run := func(e streamTraceEvent) {
			fn(trace.TopicReaderLocalBufferChangedInfo{Context: e.ctx, ReaderInfo: info, MessagesDelta: e.count})
		}
		wrapped.OnReaderLocalBufferChanged = func(i trace.TopicReaderLocalBufferChangedInfo) {
			q.Send(streamTraceEvent{run: run, ctx: i.Context, count: i.MessagesDelta})
		}
	}
	if fn := t.OnReaderMessagesReceived; fn != nil {
		run := func(e streamTraceEvent) {
			fn(trace.TopicReaderMessagesReceivedInfo{Context: e.ctx, ReaderInfo: info, Topic: e.topic, MessagesCount: e.count})
		}
		wrapped.OnReaderMessagesReceived = func(i trace.TopicReaderMessagesReceivedInfo) {
			q.Send(streamTraceEvent{run: run, ctx: i.Context, topic: i.Topic, count: i.MessagesCount})
		}
	}
	if fn := t.OnReaderMessagesDelivered; info.Listener && fn != nil {
		run := func(e streamTraceEvent) {
			fn(trace.TopicReaderMessagesDeliveredInfo{Context: e.ctx, ReaderInfo: info, Topic: e.topic, MessagesCount: e.count})
		}
		wrapped.OnReaderMessagesDelivered = func(i trace.TopicReaderMessagesDeliveredInfo) {
			q.Send(streamTraceEvent{run: run, ctx: i.Context, topic: i.Topic, count: i.MessagesCount})
		}
	}
	if fn := t.OnReaderCommittedNotify; fn != nil {
		wrapped.OnReaderCommittedNotify = func(i trace.TopicReaderCommittedNotifyInfo) {
			q.Send(streamTraceEvent{run: func(streamTraceEvent) { fn(i) }})
		}
	}
	go drainStreamTrace(q, onPanic)

	return &wrapped, q.Close
}

func drainStreamTrace(q *xsync.UnboundedChan[streamTraceEvent], onPanic func(error)) {
	q.Range(func(event streamTraceEvent) {
		// Retire once on the first panic, but keep delivering the rest of the FIFO,
		// including compensation. A broken observer must not terminate the application.
		defer func() {
			if p := recover(); p != nil && onPanic != nil {
				onPanic(fmt.Errorf("ydb: topic stream trace panic: %v", p))
				onPanic = nil
			}
		}()
		event.run(event)
	})
}
