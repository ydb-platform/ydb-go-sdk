package partition

import "github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"

// Partition provides read-only access to one partition in a Partitions snapshot.
// It is not updated when Source publishes a newer topology.
type Partition struct {
	info       topictypes.PartitionInfo
	partitions *Partitions
}

// ID returns the partition ID.
func (p Partition) ID() int64 {
	return p.info.PartitionID
}

// IsActive reports whether the partition exists and is active.
func (p Partition) IsActive() bool {
	return p.info.Active
}

// Parents returns all known parents recursively, excluding the partition itself.
// A parent referenced by ID remains in the result when its metadata is absent;
// such a parent is inactive, and traversal stops at it.
// The order is unspecified, and each partition ID occurs at most once.
// The returned list must not be modified.
func (p Partition) Parents() List {
	return p.walk(func(partition Partition) []int64 {
		return partition.info.ParentPartitionIDs
	})
}

func (p Partition) walk(next func(Partition) []int64) List {
	partitionIDs := append([]int64(nil), next(p)...)
	partitions := make(List, 0, len(partitionIDs))
	seen := map[int64]struct{}{p.ID(): {}}
	for i := 0; i < len(partitionIDs); i++ {
		partitionID := partitionIDs[i]
		if _, ok := seen[partitionID]; ok {
			continue
		}
		seen[partitionID] = struct{}{}
		partition := p.partitions.ByPartitionID(partitionID)
		partitions = append(partitions, partition)
		partitionIDs = append(partitionIDs, next(partition)...)
	}

	return partitions
}
