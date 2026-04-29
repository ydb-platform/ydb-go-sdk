package topicmultiwriter

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwritercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xlist"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

type sender struct {
	ctx                    context.Context //nolint:containedctx
	wakeupChan             empty.Chan
	onError                func(err error)
	partitionSplitReceiver *partitionSplitReceiver
	buf                    *inflightBuffer
	mu                     *xsync.Mutex
	partitions             map[int64]*PartitionInfo
	writerPool             *partitionWriterPool
}

func newSender(
	ctx context.Context,
	partitions map[int64]*PartitionInfo,
	mu *xsync.Mutex,
	buf *inflightBuffer,
	writerPool *partitionWriterPool,
	partitionSplitReceiver *partitionSplitReceiver,
	onError func(err error),
) *sender {
	return &sender{
		ctx:                    ctx,
		wakeupChan:             make(empty.Chan, 1),
		onError:                onError,
		buf:                    buf,
		mu:                     mu,
		partitions:             partitions,
		writerPool:             writerPool,
		partitionSplitReceiver: partitionSplitReceiver,
	}
}

func (s *sender) run() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.wakeupChan:
		}

		if err := s.step(); err != nil {
			s.onError(err)

			return
		}
	}
}

func (s *sender) wakeup() {
	select {
	case s.wakeupChan <- struct{}{}:
	default:
	}
}

//nolint:funlen
func (s *sender) iterateThroughMessagesIndex(
	index map[int64]xlist.List[messagePtr],
	stopFunc func(msg messagePtr) bool,
	ignorePartitionLock bool,
) error {
	var partitionsToRemove []int64

	for partitionID, list := range index {
		for iter := list.Front(); iter != nil; iter = iter.Next() {
			msg := iter.Value.Value

			partition := s.partitions[msg.PartitionID]
			if partition == nil {
				return fmt.Errorf("partition not found: %d", msg.PartitionID)
			}

			if (!ignorePartitionLock && partition.Locked) || stopFunc(iter.Value) {
				break
			}

			wr, err := s.writerPool.get(msg.PartitionID, true)
			if err != nil {
				return fmt.Errorf("failed to get writer: %w", err)
			}

			if !wr.initDone.Load() {
				break
			}

			if wr.err != nil && isOperationErrorOverloaded(wr.err) {
				s.partitionSplitReceiver.push(partitionID)

				break
			}

			if err = wr.WriteInternal(
				s.ctx,
				[]topicwritercommon.MessageWithDataContent{msg.MessageWithDataContent},
			); err != nil {
				if isOperationErrorOverloaded(err) {
					s.partitionSplitReceiver.push(partitionID)

					break
				}

				return fmt.Errorf("failed to write message: %w", err)
			}
			iter.Value.Value.sent = true
		}

		iter := list.Front()
		for list.Len() > 0 && iter != nil {
			next := iter.Next()
			if iter.Value.Value.sent {
				list.Remove(iter)
			}
			iter = next
		}

		if list.Len() == 0 {
			partitionsToRemove = append(partitionsToRemove, partitionID)
		}
	}

	for _, partitionID := range partitionsToRemove {
		delete(index, partitionID)
	}

	return nil
}

func (s *sender) step() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	checkPartitionLocked := func(partitionID int64) bool {
		partition, ok := s.partitions[partitionID]
		if !ok {
			return true
		}

		return partition.Locked
	}

	if err := s.iterateThroughMessagesIndex(
		s.buf.messagesToResendIndex,
		func(msg messagePtr) bool {
			return checkPartitionLocked(msg.Value.PartitionID)
		},
		true,
	); err != nil {
		return err
	}

	return s.iterateThroughMessagesIndex(
		s.buf.pendingMessagesIndex,
		func(msg messagePtr) bool {
			if checkPartitionLocked(msg.Value.PartitionID) {
				return true
			}

			_, ok := s.buf.messagesToResendIndex[msg.Value.PartitionID]

			return ok
		},
		false,
	)
}
