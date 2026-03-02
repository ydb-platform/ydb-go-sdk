package stubs

import (
	"context"
	"errors"
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
	onAckReceivedCallback topicwriterinternal.PublicOnAckReceivedCallback
	autoSetSeqNo          bool
	currentSeqNo          int64
}

func NewBasicWriter(
	onAckReceivedCallback topicwriterinternal.PublicOnAckReceivedCallback,
	autoSetSeqNo bool,
) *basicWriter {
	w := &basicWriter{
		onAckReceivedCallback: onAckReceivedCallback,
		currentSeqNo:          1,
		closedChan:            make(empty.Chan),
		acksChan:              make(chan int64, 100),
		autoSetSeqNo:          autoSetSeqNo,
	}

	go w.ackProcessor()

	return w
}

func (w *basicWriter) ackProcessor() {
	for ack := range w.acksChan {
		// time.Sleep(time.Millisecond * 10)
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
