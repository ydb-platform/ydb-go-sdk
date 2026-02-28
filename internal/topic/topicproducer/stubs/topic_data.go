package stubs

import "github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"

func DefaultStubTopicDescription() topictypes.TopicDescription {
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
				ToBound:            []byte("z"),
				ChildPartitionIDs:  []int64{},
				ParentPartitionIDs: []int64{},
			},
		},
	}
}
