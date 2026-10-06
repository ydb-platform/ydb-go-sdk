package topology

import "github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"

// Partitions is a read-only snapshot of one topic's partition topology.
// Its methods are safe for concurrent use.
// Values returned by its methods must not be modified.
// A snapshot is not updated when Topic loads a newer topology;
// call Topic.Partitions again to obtain the current snapshot.
type Partitions struct {
	all  List
	byID map[int64]Partition
}

// List is an ordered collection of partitions.
type List []Partition

// All returns every known partition in server order.
// The returned list must not be modified.
func (p *Partitions) All() List {
	return p.all
}

// Infos returns independent copies of all [topictypes.PartitionInfo] values in server order.
// The caller may modify the returned values without changing this snapshot.
func (p *Partitions) Infos() []topictypes.PartitionInfo {
	infos := make([]topictypes.PartitionInfo, 0, len(p.all))
	for _, partition := range p.all {
		info := partition.info
		info.ChildPartitionIDs = append([]int64(nil), info.ChildPartitionIDs...)
		info.ParentPartitionIDs = append([]int64(nil), info.ParentPartitionIDs...)
		info.FromBound = append([]byte(nil), info.FromBound...)
		info.ToBound = append([]byte(nil), info.ToBound...)
		if info.PartitionStats.LastWriteTime != nil {
			lastWriteTime := *info.PartitionStats.LastWriteTime
			info.PartitionStats.LastWriteTime = &lastWriteTime
		}
		if info.PartitionStats.MaxWriteTimeLag != nil {
			maxWriteTimeLag := *info.PartitionStats.MaxWriteTimeLag
			info.PartitionStats.MaxWriteTimeLag = &maxWriteTimeLag
		}
		infos = append(infos, info)
	}

	return infos
}

// IDs returns the partition IDs in list order.
func (p List) IDs() []int64 {
	ids := make([]int64, 0, len(p))
	for _, partition := range p {
		ids = append(ids, partition.ID())
	}

	return ids
}

// ByPartitionID returns the partition with partitionID.
// A missing partition is represented by an inactive Partition with the requested ID.
func (p *Partitions) ByPartitionID(partitionID int64) Partition {
	partition, ok := p.find(partitionID)
	if ok {
		return partition
	}

	return Partition{info: topictypes.PartitionInfo{PartitionID: partitionID}}
}

func (p *Partitions) find(partitionID int64) (Partition, bool) {
	partition, ok := p.byID[partitionID]

	return partition, ok
}
