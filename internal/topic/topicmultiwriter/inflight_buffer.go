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

func newInflightBuffer(
	ctx context.Context,
	mu *xsync.Mutex,
	cfg *MultiWriterConfig,
	getError func() error,
) *inflightBuffer {
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

func (b *inflightBuffer) pushNeedLock(msg message) {
	newElement := b.inFlightMessages.PushBack(msg)
	b.getInflightMessagesIndex(msg.PartitionID).PushBack(newElement)
	b.getPendingMessagesIndex(msg.PartitionID).PushBack(newElement)
}

// no concurrent safe
func (b *inflightBuffer) getInflightMessagesIndex(partitionID int64) xlist.List[messagePtr] {
	list, ok := b.inFlightMessagesIndex[partitionID]
	if !ok {
		list = xlist.New[messagePtr]()
		b.inFlightMessagesIndex[partitionID] = list
	}

	return list
}

// no concurrent safe
func (b *inflightBuffer) getPendingMessagesIndex(partitionID int64) xlist.List[messagePtr] {
	list, ok := b.pendingMessagesIndex[partitionID]
	if !ok {
		list = xlist.New[messagePtr]()
		b.pendingMessagesIndex[partitionID] = list
	}

	return list
}

// no concurrent safe
func (b *inflightBuffer) getMessagesToResendIndex(partitionID int64) xlist.List[messagePtr] {
	list, ok := b.messagesToResendIndex[partitionID]
	if !ok {
		list = xlist.New[messagePtr]()
		b.messagesToResendIndex[partitionID] = list
	}

	return list
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
