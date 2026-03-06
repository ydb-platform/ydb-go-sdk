package topicmultiwriter

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xlist"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

type TopicDescriber func(ctx context.Context, path string) (topictypes.TopicDescription, error)

type PartitionInfo struct {
	ID        int64
	FromBound []byte
	ToBound   []byte
	ParentID  *int64
	Children  []int64
	Locked    bool
}

func (p *PartitionInfo) Splitted() bool {
	return len(p.Children) > 0
}

type partitionShortInfo struct {
	ID        int64
	FromBound string
	ToBound   string
}

type message struct {
	topicwriterinternal.PublicMessage

	onAckCallback func()
	ackReceived   bool
	sent          bool
}

type messagePtr *xlist.Element[message]

type ack struct {
	partitionID int64
	seqNo       int64
}

type PartitionChooserStrategy uint8

const (
	PartitionChooserStrategyBound PartitionChooserStrategy = iota
	PartitionChooserStrategyHash
	PartitionChooserStrategyCustom
)

type writerWrapper struct {
	writer

	initDone atomic.Bool
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
