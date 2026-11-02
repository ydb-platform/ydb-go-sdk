package topicmultiwriter

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
)

type partitionSplitReceiver struct {
	partitionSplitCallback func(partitionID int64) error
	onError                func(err error)
	wakeupChan             empty.Chan
	partitionSplits        *guardedList[partitionSplit]
}

type partitionSplit struct {
	partitionID int64
	reason      error
	done        chan error
}

func newPartitionSplitReceiver(
	partitionSplitCallback func(partitionID int64) error,
	onError func(err error),
) *partitionSplitReceiver {
	return &partitionSplitReceiver{
		partitionSplitCallback: partitionSplitCallback,
		wakeupChan:             make(empty.Chan, 1),
		partitionSplits:        newGuardedList[partitionSplit](),
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
		for _, split := range partitionSplits {
			err := p.partitionSplitCallback(split.partitionID)
			if err != nil {
				err = fmt.Errorf("handle overloaded partition %d: %w: %w", split.partitionID, split.reason, err)
				p.onError(err)
			}
			if split.done != nil {
				split.done <- err
			}
			if err != nil {
				return
			}
		}
	}
}

func (p *partitionSplitReceiver) push(partitionID int64, reason error) {
	p.partitionSplits.PushBack(partitionSplit{partitionID: partitionID, reason: reason})
	p.wakeup()
}

func (p *partitionSplitReceiver) wait(
	ctx context.Context,
	workerDone <-chan struct{},
	partitionID int64,
	reason error,
) error {
	done := make(chan error, 1)
	p.partitionSplits.PushBack(partitionSplit{partitionID: partitionID, reason: reason, done: done})
	p.wakeup()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-workerDone:
		return ErrAlreadyClosed
	case err := <-done:
		return err
	}
}

func (p *partitionSplitReceiver) wakeup() {
	select {
	case p.wakeupChan <- struct{}{}:
	default:
	}
}
