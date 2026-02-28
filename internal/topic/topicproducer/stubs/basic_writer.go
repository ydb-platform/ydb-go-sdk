package stubs

import (
	"context"
	"errors"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

type basicWriter struct {
	closed                bool
	mu                    xsync.Mutex
	onAckReceivedCallback topicwriterinternal.PublicOnAckReceivedCallback
	currentSeqNo          int64
}

func NewBasicWriter(onAckReceivedCallback topicwriterinternal.PublicOnAckReceivedCallback) *basicWriter {
	return &basicWriter{
		onAckReceivedCallback: onAckReceivedCallback,
		currentSeqNo:          1,
	}
}

func (w *basicWriter) Write(ctx context.Context, messages ...topicwriterinternal.PublicMessage) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return errors.New("writer is closed")
	}

	for i := range messages {
		messages[i].SeqNo = w.currentSeqNo
		w.currentSeqNo++
	}

	if w.onAckReceivedCallback != nil {
		for i := range messages {
			w.onAckReceivedCallback(messages[i].SeqNo)
			time.Sleep(time.Millisecond * 10)
		}
	}

	return nil
}

func (w *basicWriter) WaitInit(ctx context.Context) error {
	time.Sleep(time.Second)
	return nil
}

func (w *basicWriter) Close(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	return nil
}
