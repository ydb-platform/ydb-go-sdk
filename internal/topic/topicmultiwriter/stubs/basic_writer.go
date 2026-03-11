package stubs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

type basicWriter struct {
	closed                bool
	closedChan            empty.Chan
	acksChan              chan int64
	mu                    xsync.Mutex
	onAckReceivedCallback func(seqNo int64)
	autoSetSeqNo          bool
	currentSeqNo          int64
	ackDelay              time.Duration
}

func NewBasicWriter(
	t testing.TB,
	onAckReceivedCallback func(seqNo int64),
	autoSetSeqNo bool,
	ackDelay time.Duration,
) *basicWriter {
	t.Helper()

	w := &basicWriter{
		onAckReceivedCallback: onAckReceivedCallback,
		currentSeqNo:          1,
		closedChan:            make(empty.Chan),
		acksChan:              make(chan int64, 100),
		autoSetSeqNo:          autoSetSeqNo,
		ackDelay:              ackDelay,
	}

	go w.ackProcessor()

	return w
}

func (w *basicWriter) ackProcessor() {
	for ack := range w.acksChan {
		if w.ackDelay > 0 {
			time.Sleep(w.ackDelay)
		}
		w.onAckReceivedCallback(ack)
	}
}

func (w *basicWriter) Write(ctx context.Context, messages []topicwriterinternal.PublicMessage) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return errors.New("writer is closed")
	}

	if w.autoSetSeqNo {
		for i := range messages {
			messages[i].SeqNo = w.currentSeqNo
			w.currentSeqNo++
		}
	}

	if w.onAckReceivedCallback != nil {
		for i := range messages {
			w.acksChan <- messages[i].SeqNo
		}
	}

	return nil
}

func (w *basicWriter) WaitInit(ctx context.Context) (topicwriterinternal.InitialInfo, error) {
	time.Sleep(time.Second)

	return topicwriterinternal.InitialInfo{
		LastSeqNum: w.currentSeqNo,
	}, nil
}

func (w *basicWriter) Close(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}

	w.closed = true
	close(w.closedChan)

	return nil
}

func (w *basicWriter) GetBufferedMessages() []topicwriterinternal.PublicMessage {
	return nil
}
