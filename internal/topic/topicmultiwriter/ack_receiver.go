package topicmultiwriter

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
)

type ackReceiver struct {
	ctx          context.Context //nolint:containedctx
	receivedAcks *guardedList[ack]
	wakeupChan   empty.Chan
	ackCallback  func(partitionID, seqNo int64)
}

func newAckReceiver(
	ctx context.Context,
	ackCallback func(partitionID, seqNo int64),
) *ackReceiver {
	return &ackReceiver{
		ctx:          ctx,
		ackCallback:  ackCallback,
		receivedAcks: newGuardedList[ack](),
		wakeupChan:   make(empty.Chan, 1),
	}
}

func (a *ackReceiver) run() {
	for {
		select {
		case <-a.ctx.Done():
			return
		case <-a.wakeupChan:
		}

		acks := a.receivedAcks.Consume()
		for _, ack := range acks {
			a.ackCallback(ack.partitionID, ack.seqNo)
		}
	}
}

func (a *ackReceiver) push(partitionID, seqNo int64) {
	a.receivedAcks.PushBack(ack{partitionID, seqNo})
	a.wakeup()
}

func (a *ackReceiver) wakeup() {
	select {
	case a.wakeupChan <- struct{}{}:
	default:
	}
}
