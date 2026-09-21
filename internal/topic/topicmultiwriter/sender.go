package topicmultiwriter

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/partition"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwritercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xlist"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

type sender struct {
	ctx        context.Context //nolint:containedctx
	wakeupChan empty.Chan
	onError    func(err error)
	source     *partition.Source
	buf        *inflightBuffer
	mu         *xsync.Mutex
	partitions map[int64]*PartitionInfo
	writerPool *partitionWriterPool
}

func newSender(
	ctx context.Context,
	partitions map[int64]*PartitionInfo,
	mu *xsync.Mutex,
	buf *inflightBuffer,
	writerPool *partitionWriterPool,
	source *partition.Source,
	onError func(err error),
) *sender {
	return &sender{
		ctx:        ctx,
		wakeupChan: make(empty.Chan, 1),
		onError:    onError,
		buf:        buf,
		mu:         mu,
		partitions: partitions,
		writerPool: writerPool,
		source:     source,
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
	partitions *partition.Partitions,
	index map[int64]xlist.List[messagePtr],
	resend bool,
) error {
	var partitionsToRemove []int64

	for partitionID, list := range index {
		for iter := list.Front(); iter != nil; iter = iter.Next() {
			msg := iter.Value.Value

			partition := s.partitions[msg.PartitionID]
			if partition == nil {
				return fmt.Errorf("partition not found: %d", msg.PartitionID)
			}

			canSend := s.canSendNeedLock(partitions, msg.PartitionID, resend)
			if !canSend {
				break
			}

			wr, err := s.writerPool.get(msg.PartitionID)
			if err != nil {
				return fmt.Errorf("failed to get writer: %w", err)
			}

			if !wr.initDone.Load() {
				break
			}

			if err := wr.getInitErr(); err != nil {
				if s.source.NotifySessionError(s.ctx, partitionID, err) != nil {
					partition.Locked = true

					break
				}

				return fmt.Errorf("writer init failed for partition %d: %w", msg.PartitionID, err)
			}

			if err = wr.WriteInternal(
				s.ctx,
				[]topicwritercommon.MessageWithDataContent{msg.MessageWithDataContent},
			); err != nil {
				if s.source.NotifySessionError(s.ctx, partitionID, err) != nil {
					partition.Locked = true

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

// canSendNeedLock combines shared topology with this writer's outstanding messages.
func (s *sender) canSendNeedLock(partitions *partition.Partitions, partitionID int64, resend bool) bool {
	state := s.partitions[partitionID]
	if state.Locked {
		return false
	}
	topicPartition := partitions.ByPartitionID(partitionID)
	if !topicPartition.IsActive() {
		return false
	}

	// Source may publish a new route before this writer receives its split event.
	// Neither resends nor new messages may overtake outstanding messages of a parent.
	for _, parent := range topicPartition.Parents() {
		if pending, ok := s.buf.inFlightMessagesIndex[parent.ID()]; ok && pending.Len() > 0 {
			return false
		}
	}
	if resend {
		return true
	}

	_, hasResends := s.buf.messagesToResendIndex[partitionID]

	return state.PendingResend == 0 && !hasResends
}

func (s *sender) step() error {
	partitions, err := s.source.Partitions(s.ctx)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.iterateThroughMessagesIndex(partitions, s.buf.messagesToResendIndex, true); err != nil {
		return err
	}

	return s.iterateThroughMessagesIndex(partitions, s.buf.pendingMessagesIndex, false)
}
