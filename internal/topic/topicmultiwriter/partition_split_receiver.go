package topicmultiwriter

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
)

type partitionSplitReceiver struct {
	partitionSplitCallback func(partitionID int64) error
	onError                func(err error)
	wakeupChan             empty.Chan
	partitionSplits        *guardedList[int64]
}

func newPartitionSplitReceiver(
	partitionSplitCallback func(partitionID int64) error,
	onError func(err error),
) *partitionSplitReceiver {
	return &partitionSplitReceiver{
		partitionSplitCallback: partitionSplitCallback,
		wakeupChan:             make(empty.Chan, 1),
		partitionSplits:        newGuardedList[int64](),
		onError:                onError,
	}
}

func (p *partitionSplitReceiver) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.wakeupChan:
		}

		partitionSplits := p.partitionSplits.Consume()
		for _, partitionID := range partitionSplits {
			if err := p.partitionSplitCallback(partitionID); err != nil {
				p.onError(err)

				return
			}
		}
	}
}

func (p *partitionSplitReceiver) push(partitionID int64) {
	p.partitionSplits.PushBack(partitionID)
	p.wakeup()
}

func (p *partitionSplitReceiver) wakeup() {
	select {
	case p.wakeupChan <- struct{}{}:
	default:
	}
}
