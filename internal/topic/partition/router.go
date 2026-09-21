package partition

import (
	"context"
	"fmt"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

// Chooser preserves the existing user-supplied partition selection interface.
// Concurrent calls are not supported.
// Implementations must not modify PartitionInfo values passed to AddNewPartitions
// or data referenced by their fields.
// After RemovePartition returns, ChoosePartition must not return that partition
// until it is passed to AddNewPartitions again.
type Chooser interface {
	ChoosePartition(msg topicwriterinternal.PublicMessage) (int64, error)
	AddNewPartitions(partitions ...topictypes.PartitionInfo) error
	RemovePartition(partitionID int64)
}

// Router binds a writer's chooser to its topic's shared Source. Obtain it through Source.NewRouter.
// Kafka Hash assumes fixed topology; bound-based routing applies topology changes discovered from session errors.
// It synchronizes chooser calls with topology updates published by Source.
// If a topology update fails, Router stops choosing partitions because the chooser may contain
// a stale or partially updated partition set.
// Its methods are safe for concurrent use and require no external locking.
type Router struct {
	ctx          context.Context //nolint:containedctx // Router lifetime context.
	chooser      Chooser
	partitions   *Partitions
	subscription *subscription
	updateError  error
	mu           sync.Mutex
}

// ChoosePartition selects among registered partitions with internal synchronization.
// After a topology update fails, it returns that update error without calling the chooser.
func (r *Router) ChoosePartition(msg topicwriterinternal.PublicMessage) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.updateError != nil {
		return 0, r.updateError
	}
	var (
		partitionID int64
		err         error
	)
	if r.chooser == nil {
		partitionID = msg.PartitionID
	} else {
		partitionID, err = r.chooser.ChoosePartition(msg)
		if err != nil {
			return 0, err
		}
	}
	partition, ok := r.partitions.find(partitionID)
	if !ok || !partition.IsActive() {
		return 0, fmt.Errorf("partition %d does not exist or is inactive", partitionID)
	}

	return partitionID, nil
}

// WaitForRouteChange waits for the consequences of a topology change already tracked and published by Source.
// On success, partitionID identifies a partition replaced by its children, so the Router owner can recover its
// own in-flight state. It returns a Router-specific update error when this Router's chooser could not be updated,
// even if other Routers sharing the Source were updated successfully. WaitForRouteChange does not refresh topology.
// Waiting ends when the lifetime context passed to Source.NewRouter is canceled.
// Only one goroutine may call WaitForRouteChange for a Router.
func (r *Router) WaitForRouteChange() (partitionID int64, err error) {
	event, err := r.subscription.wait()
	if err != nil {
		return 0, err
	}

	return event.partitionID, event.err
}

func (r *Router) updatePartitions(partitions *Partitions) (previous *Partitions, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.updatePartitionsNeedLock(partitions)
}

func (r *Router) updatePartitionsNeedLock(partitions *Partitions) (previous *Partitions, err error) {
	previous = r.partitions
	select {
	case <-r.ctx.Done():
		return previous, nil
	default:
	}
	if r.updateError != nil {
		return previous, r.updateError
	}
	if r.chooser != nil {
		toAdd := make([]topictypes.PartitionInfo, 0)
		for _, partition := range partitions.all {
			previous, exists := r.partitions.find(partition.ID())
			if partition.IsActive() && (!exists || !previous.IsActive()) {
				toAdd = append(toAdd, partition.info)
			}
		}
		if err := r.chooser.AddNewPartitions(toAdd...); err != nil {
			r.updateError = err

			return previous, err
		}
		for _, partition := range r.partitions.all {
			current, exists := partitions.find(partition.ID())
			if partition.IsActive() && (!exists || !current.IsActive()) {
				r.chooser.RemovePartition(partition.ID())
			}
		}
	}
	r.partitions = partitions

	return previous, nil
}

func (r *Router) fail(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.updateError == nil {
		r.updateError = err
	}
}
