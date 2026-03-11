package stubs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xlist"
)

// MessagesBeforeOverloaded is the number of messages the stub accepts before returning OVERLOADED.
const MessagesBeforeOverloaded = 100

type writerWithAutopartitioning struct {
	closed                bool
	mu                    *sync.Mutex
	onAckReceivedCallback func(seqNo int64)
	retrySettings         topic.RetrySettings
	autoSetSeqNo          bool
	currentSeqNo          int64
	messagesWritten       int64
	partitionID           int64
	onSplit               func(partitionID int64)
	updateSeqNo           func(producerID string, seqNo int64)
	producerIDPrefix      string
	wakeUpChan            chan empty.Struct
	closeChan             chan empty.Struct
	messagesToProcess     xlist.List[topicwriterinternal.PublicMessage]
	splitted              atomic.Bool
}

// NewWriterWithAutopartitioning creates a stub writer that behaves like BasicWriter
// but returns OVERLOADED (StatusIds_OVERLOADED) after MessagesBeforeOverloaded messages.
// When OVERLOADED is returned, onSplit(partitionID) is called so the topic client stub
// can add child partitions to the next Describe response (split bounds: [from, mid), [mid, to)).
func NewWriterWithAutopartitioning(
	t testing.TB,
	onAckReceivedCallback func(seqNo int64),
	retrySettings topic.RetrySettings,
	autoSetSeqNo bool,
	partitionID int64,
	onSplit func(partitionID int64),
	updateSeqNo func(producerID string, seqNo int64),
	maxSeqNo int64,
	producerIDPrefix string,
) *writerWithAutopartitioning {
	t.Helper()

	mu := &sync.Mutex{}

	w := &writerWithAutopartitioning{
		onAckReceivedCallback: onAckReceivedCallback,
		retrySettings:         retrySettings,
		currentSeqNo:          maxSeqNo,
		autoSetSeqNo:          autoSetSeqNo,
		partitionID:           partitionID,
		onSplit:               onSplit,
		updateSeqNo:           updateSeqNo,
		producerIDPrefix:      producerIDPrefix,
		mu:                    mu,
		wakeUpChan:            make(chan empty.Struct, 1),
		closeChan:             make(chan empty.Struct),
		messagesToProcess:     xlist.New[topicwriterinternal.PublicMessage](),
	}

	go w.work()

	return w
}

func (w *writerWithAutopartitioning) wakeUp() {
	select {
	case w.wakeUpChan <- empty.Struct{}:
	default:
	}
}

func (w *writerWithAutopartitioning) work() {
	for {
		select {
		case <-w.wakeUpChan:
		case <-w.closeChan:
			return
		}

		w.processMessages()
		if w.splitted.Load() {
			return
		}
	}
}

func (w *writerWithAutopartitioning) processMessages() {
	var (
		acks           []int64
		needOverloaded bool
	)

	w.mu.Lock()
	for w.messagesToProcess.Len() > 0 {
		iter := w.messagesToProcess.Front()
		w.messagesToProcess.Remove(iter)

		if w.messagesWritten >= MessagesBeforeOverloaded {
			if w.retrySettings.CheckError != nil {
				needOverloaded = true
			}

			w.splitted.Store(true)
			w.onSplit(w.partitionID)

			break
		}

		if w.autoSetSeqNo {
			w.currentSeqNo++
			iter.Value.SeqNo = w.currentSeqNo
		} else {
			w.currentSeqNo = iter.Value.SeqNo
		}

		w.messagesWritten++
		w.updateSeqNo(fmt.Sprintf("%s_%d", w.producerIDPrefix, w.partitionID), iter.Value.SeqNo)
		acks = append(acks, iter.Value.SeqNo)
	}
	w.mu.Unlock()

	for _, seqNo := range acks {
		if w.onAckReceivedCallback != nil {
			w.onAckReceivedCallback(seqNo)
		}
	}

	if needOverloaded {
		w.retrySettings.CheckError(topic.PublicCheckErrorRetryArgs{
			Error: xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_OVERLOADED)),
		})
	}
}

func (w *writerWithAutopartitioning) Write(ctx context.Context, messages []topicwriterinternal.PublicMessage) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return errors.New("writer is closed")
	}

	for _, message := range messages {
		w.messagesToProcess.PushBack(message)
	}
	w.wakeUp()

	return nil
}

func (w *writerWithAutopartitioning) WaitInit(ctx context.Context) (topicwriterinternal.InitialInfo, error) {
	time.Sleep(time.Second)

	return topicwriterinternal.InitialInfo{
		LastSeqNum: w.currentSeqNo,
	}, nil
}

func (w *writerWithAutopartitioning) Close(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}

	w.closed = true
	close(w.closeChan)

	return nil
}

func (w *writerWithAutopartitioning) GetBufferedMessages() []topicwriterinternal.PublicMessage {
	return nil
}
