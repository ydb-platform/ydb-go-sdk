package topicmultiwriter

import (
	"sync/atomic"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwritercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xlist"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

// PartitionInfo stores one writer's state; topology belongs to partition.Source.
type PartitionInfo struct {
	Locked          bool
	PendingResend   int
	CachedMaxSeqNo  int64
	LastQueuedSeqNo int64
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
