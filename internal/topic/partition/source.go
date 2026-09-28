package partition

import (
	"context"
	"errors"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

var errReplacementNotPublished = errors.New("partition replacement was not published")

// TopicDescriber loads topic metadata without depending on client or writer options.
type TopicDescriber func(ctx context.Context, path string) (topictypes.TopicDescription, error)

// Source caches metadata for one topic and is shared by that topic's writers in one client.
// Obtain a Source through Sources.Get; its zero value is not usable.
// Cached metadata has no time-based expiration or periodic refresh.
// Reloads are triggered by explicit invalidation or a reported inactive partition.
type Source struct {
	topicPath           string
	describe            TopicDescriber
	partitions          *Partitions
	partitionsLoad      *partitionsLoad
	pendingReplacements map[int64]struct{}
	mu                  sync.Mutex
}

type partitionsLoad struct {
	done        chan struct{}
	invalidated bool
	err         error
}

// Partitions returns a current read-only topology snapshot.
// A previously returned snapshot is not updated when a newer topology is loaded.
// After NotifySessionError accepts an inactive-partition error, Partitions waits until
// the replacement of every reported partition is present in topic metadata.
func (s *Source) Partitions(ctx context.Context) (*Partitions, error) {
	return retry.RetryWithResult(ctx, func(ctx context.Context) (*Partitions, error) {
		partitions, err := s.partitionsOnce(ctx)
		if err == nil {
			return partitions, nil
		}
		if errors.Is(err, errReplacementNotPublished) {
			return nil, retry.RetryableError(errReplacementNotPublished, retry.WithBackoff(retry.TypeFastBackoff))
		}

		return nil, xerrors.Unretryable(err)
	}, retry.WithIdempotent(true))
}

// NotifySessionError invalidates cached metadata when err reports that partitionID became inactive.
// It returns whether the error was accepted as a topology change. The next Partitions call waits
// until metadata contains the complete replacement of partitionID.
func (s *Source) NotifySessionError(partitionID int64, err error) bool {
	if !xerrors.IsOperationErrorTopicPartitionInactive(err) {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if replacementPublished(s.partitions, partitionID) {
		return true
	}
	if s.pendingReplacements == nil {
		s.pendingReplacements = make(map[int64]struct{})
	}
	s.pendingReplacements[partitionID] = struct{}{}
	s.invalidateNeedLock()

	return true
}

// Invalidate marks this topic's cached metadata for reload without doing network I/O.
func (s *Source) Invalidate() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.invalidateNeedLock()
}

func (s *Source) invalidateNeedLock() {
	s.partitions = nil
	if s.partitionsLoad != nil {
		s.partitionsLoad.invalidated = true
	}
}

func (s *Source) partitionsOnce(ctx context.Context) (*Partitions, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		partitions, load, created := s.cachedPartitionsOrLoad()
		if partitions != nil {
			return partitions, nil
		}
		if !created {
			select {
			case <-load.done:
				if load.err != nil && !isContextError(load.err) {
					return nil, load.err
				}

				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		description, err := s.describe(ctx, s.topicPath)
		partitions = partitionsFromDescription(description)
		published, finishErr := s.finishPartitionsLoad(load, partitions, err)
		if err != nil {
			return nil, err
		}
		if finishErr != nil {
			return nil, finishErr
		}
		if published {
			return partitions, nil
		}
	}
}

func (s *Source) cachedPartitionsOrLoad() (partitions *Partitions, load *partitionsLoad, created bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.partitions != nil {
		return s.partitions, nil, false
	}
	if s.partitionsLoad != nil {
		return nil, s.partitionsLoad, false
	}
	load = &partitionsLoad{done: make(chan struct{})}
	s.partitionsLoad = load

	return nil, load, true
}

func (s *Source) finishPartitionsLoad(
	load *partitionsLoad,
	partitions *Partitions,
	describeErr error,
) (published bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.finishPartitionsLoadNeedLock(load)

	if describeErr != nil {
		load.err = describeErr

		return false, describeErr
	}
	if load.invalidated {
		return false, nil
	}
	for partitionID := range s.pendingReplacements {
		if !replacementPublished(partitions, partitionID) {
			load.err = errReplacementNotPublished

			return false, errReplacementNotPublished
		}
	}

	s.partitions = partitions
	clear(s.pendingReplacements)

	return true, nil
}

func (s *Source) finishPartitionsLoadNeedLock(load *partitionsLoad) {
	if s.partitionsLoad == load {
		s.partitionsLoad = nil
	}
	close(load.done)
}

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func partitionsFromDescription(description topictypes.TopicDescription) *Partitions {
	partitions := &Partitions{
		all:  make(List, 0, len(description.Partitions)),
		byID: make(map[int64]Partition, len(description.Partitions)),
	}
	for _, partition := range description.Partitions {
		topicPartition := Partition{
			info:       partition,
			partitions: partitions,
		}
		partitions.all = append(partitions.all, topicPartition)
		partitions.byID[topicPartition.ID()] = topicPartition
	}

	return partitions
}

func replacementPublished(partitions *Partitions, partitionID int64) bool {
	if partitions == nil {
		return false
	}
	parent, ok := partitions.find(partitionID)
	if !ok || parent.IsActive() || len(parent.info.ChildPartitionIDs) == 0 {
		return false
	}
	path := map[int64]struct{}{partitionID: {}}
	for _, childID := range parent.info.ChildPartitionIDs {
		if !replacementBranchPublished(partitions, childID, path) {
			return false
		}
	}

	return true
}

func replacementBranchPublished(partitions *Partitions, partitionID int64, path map[int64]struct{}) bool {
	partition, ok := partitions.find(partitionID)
	if !ok {
		return false
	}
	if partition.IsActive() {
		return true
	}
	if len(partition.info.ChildPartitionIDs) == 0 {
		return false
	}
	if _, ok = path[partitionID]; ok {
		return false
	}
	path[partitionID] = struct{}{}
	defer delete(path, partitionID)

	for _, childID := range partition.info.ChildPartitionIDs {
		if !replacementBranchPublished(partitions, childID, path) {
			return false
		}
	}

	return true
}
