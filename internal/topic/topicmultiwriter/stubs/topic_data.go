package stubs

import (
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

// Hash bounds for 5 partitions: MurmurHash64 (default hasher) returns 8 bytes in big-endian.
// These bounds split the uint64 range [0, 2^64-1] into 5 equal segments so hashed keys
// are distributed evenly across partitions.
var (
	bound1of5 = []byte{0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33} // 1/5 of 2^64
	bound2of5 = []byte{0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66} // 2/5
	bound3of5 = []byte{0x99, 0x99, 0x99, 0x99, 0x99, 0x99, 0x99, 0x99} // 3/5
	bound4of5 = []byte{0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc} // 4/5
	bound5of5 = []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff} // 2^64-1
)

func DefaultStubTopicDescription(t testing.TB) topictypes.TopicDescription {
	t.Helper()

	return topictypes.TopicDescription{
		Path: "test/topic",
		PartitionSettings: topictypes.PartitionSettings{
			AutoPartitioningSettings: topictypes.AutoPartitioningSettings{
				AutoPartitioningStrategy: topictypes.AutoPartitioningStrategyDisabled,
			},
		},
		Partitions: []topictypes.PartitionInfo{
			{
				PartitionID:        1,
				FromBound:          []byte{},
				ToBound:            bound1of5,
				ChildPartitionIDs:  []int64{},
				ParentPartitionIDs: []int64{},
				Active:             true,
			},
			{
				PartitionID:        2,
				FromBound:          bound1of5,
				ToBound:            bound2of5,
				ChildPartitionIDs:  []int64{},
				ParentPartitionIDs: []int64{},
				Active:             true,
			},
			{
				PartitionID:        3,
				FromBound:          bound2of5,
				ToBound:            bound3of5,
				ChildPartitionIDs:  []int64{},
				ParentPartitionIDs: []int64{},
				Active:             true,
			},
			{
				PartitionID:        4,
				FromBound:          bound3of5,
				ToBound:            bound4of5,
				ChildPartitionIDs:  []int64{},
				ParentPartitionIDs: []int64{},
				Active:             true,
			},
			{
				PartitionID:        5,
				FromBound:          bound4of5,
				ToBound:            bound5of5,
				ChildPartitionIDs:  []int64{},
				ParentPartitionIDs: []int64{},
				Active:             true,
			},
		},
	}
}
