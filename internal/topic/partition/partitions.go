package partition

import "github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"

// Partitions is a read-only snapshot of one topic's partition topology.
// Its methods are safe for concurrent use.
// Values returned by its methods must not be modified.
// A snapshot is not updated when Source publishes a newer topology;
// call Source.Partitions again to obtain the current snapshot.
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

func (p *Partitions) activeInfos() []topictypes.PartitionInfo {
	infos := make([]topictypes.PartitionInfo, 0, len(p.all))
	for _, partition := range p.all {
		if partition.IsActive() {
			infos = append(infos, partition.info)
		}
	}

	return infos
}
