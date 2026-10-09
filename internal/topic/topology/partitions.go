package topology

import "github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"

// Partitions is a read-only snapshot of one topic's partition topology.
// Its methods are safe for concurrent use.
// A snapshot is not updated when Topic loads a newer topology;
// call Topic.Partitions again to obtain the current snapshot.
type Partitions struct {
	infos []topictypes.PartitionInfo
	byID  map[int64]int
}

// Infos returns independent copies of all [topictypes.PartitionInfo] values in server order.
// The caller may modify the returned values without changing this snapshot.
func (p *Partitions) Infos() []topictypes.PartitionInfo {
	infos := make([]topictypes.PartitionInfo, 0, len(p.infos))
	for _, partition := range p.infos {
		info := partition
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

func (p *Partitions) find(partitionID int64) (topictypes.PartitionInfo, bool) {
	index, ok := p.byID[partitionID]
	if !ok {
		return topictypes.PartitionInfo{}, false
	}

	return p.infos[index], true
}
