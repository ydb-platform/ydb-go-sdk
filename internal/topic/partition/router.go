package partition

import (
	"errors"
	"fmt"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

var errNilPartitions = errors.New("partitions snapshot is nil")

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

// Router binds a writer's chooser to an explicitly supplied topology snapshot.
// It is independent from Source and changes only when its owner calls Apply.
// If a topology update fails, Router stops choosing partitions because the chooser
// may contain a stale or partially updated partition set.
// Its methods are safe for concurrent use and require no external locking.
type Router struct {
	chooser     Chooser
	partitions  *Partitions
	updateError error
	mu          sync.Mutex
}

// NewRouter initializes chooser with active partitions from partitions.
func NewRouter(partitions *Partitions, chooser Chooser) (*Router, error) {
	if partitions == nil {
		return nil, errNilPartitions
	}
	if chooser != nil {
		activePartitions := partitions.activeInfos()
		if len(activePartitions) > 0 {
			if err := chooser.AddNewPartitions(activePartitions...); err != nil {
				return nil, err
			}
		}
	}

	return &Router{chooser: chooser, partitions: partitions}, nil
}

// ChoosePartition selects among partitions in the last successfully applied snapshot.
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

// Apply reconciles the chooser with partitions and makes the snapshot current.
func (r *Router) Apply(partitions *Partitions) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if partitions == nil {
		return errNilPartitions
	}
	if r.updateError != nil {
		return r.updateError
	}
	if r.chooser == nil {
		r.partitions = partitions

		return nil
	}

	toAdd := make([]topictypes.PartitionInfo, 0)
	for _, partition := range partitions.all {
		previous, exists := r.partitions.find(partition.ID())
		if partition.IsActive() && (!exists || !previous.IsActive()) {
			toAdd = append(toAdd, partition.info)
		}
	}
	if len(toAdd) > 0 {
		if err := r.chooser.AddNewPartitions(toAdd...); err != nil {
			r.updateError = err

			return err
		}
	}
	for _, partition := range r.partitions.all {
		current, exists := partitions.find(partition.ID())
		if partition.IsActive() && (!exists || !current.IsActive()) {
			r.chooser.RemovePartition(partition.ID())
		}
	}
	r.partitions = partitions

	return nil
}
