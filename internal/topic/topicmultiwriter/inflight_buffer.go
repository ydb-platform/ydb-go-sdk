package topicmultiwriter

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xlist"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

type inflightBuffer struct {
	ctx                   context.Context //nolint:containedctx
	mu                    *xsync.Mutex
	inFlightMessages      xlist.List[message]
	inFlightMessagesIndex map[int64]xlist.List[messagePtr]
	pendingMessagesIndex  map[int64]xlist.List[messagePtr]
	messagesToResendIndex map[int64]xlist.List[messagePtr]
	messagesSema          empty.Chan
	getError              func() error
}

func newInflightBuffer(ctx context.Context, mu *xsync.Mutex, cfg *MultiWriterConfig, getError func() error) *inflightBuffer {
	return &inflightBuffer{
		ctx:                   ctx,
		mu:                    mu,
		inFlightMessages:      xlist.New[message](),
		inFlightMessagesIndex: make(map[int64]xlist.List[messagePtr]),
		pendingMessagesIndex:  make(map[int64]xlist.List[messagePtr]),
		messagesToResendIndex: make(map[int64]xlist.List[messagePtr]),
		messagesSema:          make(empty.Chan, cfg.MaxQueueLen),
		getError:              getError,
	}
}

func (b *inflightBuffer) acquireMessage(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.ctx.Done():
		return b.getError()
	case b.messagesSema <- struct{}{}:
		return nil
	}
}

func (b *inflightBuffer) releaseMessage() {
	<-b.messagesSema
}

func (b *inflightBuffer) pushNeedLock(msg message) error {
	newElement := b.inFlightMessages.PushBack(msg)
	b.addToInFlightMessagesIndex(newElement, msg.PartitionID, false)
	b.addToPendingMessagesIndex(newElement, msg.PartitionID, false)

	return nil
}

// no concurrent safe
func (b *inflightBuffer) addToInFlightMessagesIndex(newElement messagePtr, toPartition int64, toFront bool) {
	list, ok := b.inFlightMessagesIndex[toPartition]
	if !ok {
		list = xlist.New[messagePtr]()
		b.inFlightMessagesIndex[toPartition] = list
	}

	if toFront {
		list.PushFront(newElement)
	} else {
		list.PushBack(newElement)
	}
}

// no concurrent safe
func (b *inflightBuffer) addToPendingMessagesIndex(newElement messagePtr, toPartition int64, toFront bool) {
	list, ok := b.pendingMessagesIndex[toPartition]
	if !ok {
		list = xlist.New[messagePtr]()
		b.pendingMessagesIndex[toPartition] = list
	}

	if toFront {
		list.PushFront(newElement)
	} else {
		list.PushBack(newElement)
	}
}

// no concurrent safe
func (b *inflightBuffer) addToMessagesToResendIndex(newElement messagePtr, toPartition int64, toFront bool) {
	list, ok := b.messagesToResendIndex[toPartition]
	if !ok {
		list = xlist.New[messagePtr]()
		b.messagesToResendIndex[toPartition] = list
	}

	if toFront {
		list.PushFront(newElement)
	} else {
		list.PushBack(newElement)
	}
}

// no concurrent safe
// must be guarded by w.mu
func (b *inflightBuffer) sweep() {
	for b.inFlightMessages.Len() > 0 {
		front := b.inFlightMessages.Front()
		if !front.Value.ackReceived {
			return
		}

		if front.Value.onAckCallback != nil {
			front.Value.onAckCallback()
		}

		b.inFlightMessages.Remove(front)
		b.releaseMessage()
	}
}
