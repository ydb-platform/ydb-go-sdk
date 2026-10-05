package topicmultiwriter

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwritercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xlist"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

type TopicDescriber func(ctx context.Context, path string) (topictypes.TopicDescription, error)

type PartitionInfo struct {
	topictypes.PartitionInfo

	Locked          bool
	PendingResend   int
	CachedMaxSeqNo  int64
	LastQueuedSeqNo int64
}

func (p *PartitionInfo) Splitted() bool {
	return len(p.ChildPartitionIDs) > 0
}

type message struct {
	topicwritercommon.MessageWithDataContent

	onAckCallback func()
	ackReceived   bool
	sent          bool
}

type messagePtr *xlist.Element[message]

type ack struct {
	partitionID int64
	seqNo       int64
}

type writerWrapper struct {
	writer

	initDone atomic.Bool
	initErr  atomic.Value
	direct   bool
}

func (w *writerWrapper) setInitErr(err error) {
	if err != nil {
		w.initErr.Store(err)
	}
}

func (w *writerWrapper) getInitErr() error {
	err, _ := w.initErr.Load().(error)

	return err
}

type idleWriterInfo struct {
	partitionID int64
	deadline    time.Time
}

type WriteStats struct {
	MessagesWritten  int64
	LastWrittenSeqNo int64
}

type guardedList[T any] struct {
	xlist.List[T]

	mu xsync.Mutex
}

func (l *guardedList[T]) PushBack(v T) *xlist.Element[T] {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.List.PushBack(v)
}

func (l *guardedList[T]) Consume() []T {
	l.mu.Lock()
	defer l.mu.Unlock()

	result := make([]T, 0, l.Len())
	for iter := l.Front(); iter != nil; iter = iter.Next() {
		result = append(result, iter.Value)
	}
	l.Clear()

	return result
}

func newGuardedList[T any]() *guardedList[T] {
	return &guardedList[T]{
		List: xlist.New[T](),
	}
}
