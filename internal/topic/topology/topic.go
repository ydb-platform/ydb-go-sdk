package topology

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"golang.org/x/sync/singleflight"

	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

// TopicDescriber loads topic metadata without depending on client or writer options.
type TopicDescriber func(ctx context.Context, path string) (topictypes.TopicDescription, error)

// Topic caches metadata for one topic and is shared by that topic's writers in one client.
// Obtain a Topic through Registry.Get; its zero value is not usable.
// Cached metadata has no time-based expiration or periodic refresh.
// Reloads are triggered by explicit invalidation or a reported inactive partition.
type Topic struct {
	topicPath  string
	describe   TopicDescriber
	partitions *Partitions
	// pendingReplacements prevents caching metadata that still routes writes to partitions reported inactive.
	// It tracks every concurrently reported partition until one snapshot contains a complete active replacement
	// subtree for each of them.
	pendingReplacements map[int64]struct{}
	updates             singleflight.Group
	// reloadRequested prevents an explicit Invalidate from being lost while Describe is in flight.
	// Inactive partition reports do not set it: pendingReplacements are checked atomically when publishing a snapshot.
	reloadRequested bool
	mu              sync.Mutex
}

// Partitions returns a current read-only topology snapshot.
// A previously returned snapshot is not updated when a newer topology is loaded.
// After ReportInactivePartition, Partitions waits until
// the replacement of every reported partition is present in topic metadata.
func (s *Topic) Partitions(ctx context.Context) (*Partitions, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		s.mu.Lock()
		partitions := s.partitions
		s.mu.Unlock()
		if partitions != nil {
			return partitions, nil
		}

		update := s.updates.DoChan(s.topicPath, func() (any, error) {
			s.mu.Lock()
			partitions := s.partitions
			s.mu.Unlock()
			if partitions != nil {
				return partitions, nil
			}

			return s.updatePartitions(ctx)
		})
		select {
		case result := <-update:
			if result.Err == nil {
				partitions, ok := result.Val.(*Partitions)
				if !ok {
					return nil, fmt.Errorf("unexpected partitions result type %T", result.Val)
				}

				return partitions, nil
			}
			if !isContextError(result.Err) {
				return nil, result.Err
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// ReportInactivePartition invalidates cached metadata after partitionID becomes inactive.
// The next Partitions call waits until metadata contains its complete replacement.
func (s *Topic) ReportInactivePartition(partitionID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if replacementPublished(s.partitions, partitionID) {
		return
	}
	if s.pendingReplacements == nil {
		s.pendingReplacements = make(map[int64]struct{})
	}
	// Describe may keep returning the old topology for a while after a partition becomes inactive.
	// Keep the partition pending so such a snapshot cannot be cached and used by a new Router.
	s.pendingReplacements[partitionID] = struct{}{}
	s.partitions = nil
}

// Invalidate marks this topic's cached metadata for reload without doing network I/O.
func (s *Topic) Invalidate() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.partitions = nil
	s.reloadRequested = true
}

func (s *Topic) updatePartitions(ctx context.Context) (*Partitions, error) {
	for {
		s.mu.Lock()
		s.reloadRequested = false
		s.mu.Unlock()

		published := false
		partitions, err := retry.RetryWithResult(ctx, func(ctx context.Context) (*Partitions, error) {
			description, err := s.describe(ctx, s.topicPath)
			if err != nil {
				return nil, err
			}
			partitions := partitionsFromDescription(description)

			s.mu.Lock()
			defer s.mu.Unlock()
			if s.reloadRequested {
				return partitions, nil
			}
			if err = s.partitionReplacementRetryErrorNeedLock(partitions); err != nil {
				return nil, err
			}
			s.partitions = partitions
			clear(s.pendingReplacements)
			published = true

			return partitions, nil
		}, retry.WithIdempotent(true))
		if err != nil {
			return nil, err
		}
		if published {
			return partitions, nil
		}
	}
}

func (s *Topic) partitionReplacementRetryErrorNeedLock(partitions *Partitions) error {
	// Several writers may report different inactive partitions before metadata catches up.
	// Publish the snapshot only after it contains a complete replacement for all of them.
	for partitionID := range s.pendingReplacements {
		if !replacementPublished(partitions, partitionID) {
			return retry.RetryableError(
				fmt.Errorf("replacement for partition %d is not visible yet", partitionID),
				retry.WithBackoff(retry.TypeFastBackoff),
			)
		}
	}

	return nil
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

// replacementPublished reports whether partitionID is inactive and every branch of its replacement
// subtree ends in an active partition. This prevents publishing a topology with missing or intermediate children.
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
