package stubs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmultiwriter/partitionchooser"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwritercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

// BasicWriterOption configures stub basicWriter behavior.
type BasicWriterOption func(*basicWriter)

// WithRequireChoosePartitionKeyMetadata makes WriteInternal fail the test if any message
// lacks non-empty metadata entry choose_partition_key (as set by BoundPartitionChooser).
func WithRequireChoosePartitionKeyMetadata(tb testing.TB) BasicWriterOption {
	return func(w *basicWriter) {
		w.tb = tb
		w.requireChoosePartitionKey = true
	}
}

type basicWriter struct {
	closed                bool
	closedChan            empty.Chan
	acksChan              chan int64
	mu                    xsync.Mutex
	onAckReceivedCallback func(seqNo int64)
	autoSetSeqNo          bool
	currentSeqNo          int64
	ackDelay              time.Duration

	tb                        testing.TB
	requireChoosePartitionKey bool
}

func NewBasicWriter(
	t testing.TB,
	onAckReceivedCallback func(seqNo int64),
	autoSetSeqNo bool,
	ackDelay time.Duration,
	opts ...BasicWriterOption,
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

	for _, opt := range opts {
		opt(w)
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

func (w *basicWriter) WriteInternal(ctx context.Context, messages []topicwritercommon.MessageWithDataContent) error {
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

	if w.requireChoosePartitionKey {
		for i := range messages {
			require.NotNil(w.tb, messages[i].Metadata, "message %d: metadata must be set", i)
			v, ok := messages[i].Metadata[partitionchooser.ChoosePartitionKeyMetadataKey]
			require.True(w.tb, ok && len(v) > 0,
				"message %d: metadata %q must be non-empty", i, partitionchooser.ChoosePartitionKeyMetadataKey)
		}
	}

	if w.onAckReceivedCallback != nil {
		for i := range messages {
			w.acksChan <- messages[i].SeqNo
		}
	}

	return nil
}

func (w *basicWriter) WaitInitInfo(ctx context.Context) (topicwriterinternal.InitialInfo, error) {
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
