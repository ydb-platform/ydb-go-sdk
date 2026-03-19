package partitionchooser

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicpartitions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

type byPartitionIDPartitionChooser struct{}

func NewByPartitionIDPartitionChooser() *byPartitionIDPartitionChooser {
	return &byPartitionIDPartitionChooser{}
}

func (c *byPartitionIDPartitionChooser) ChoosePartition(msg topicpartitions.Message) (int64, error) {
	return msg.PartitionID, nil
}

func (c *byPartitionIDPartitionChooser) AddNewPartitions(partitions ...topictypes.PartitionInfo) error {
	return nil
}

func (c *byPartitionIDPartitionChooser) RemovePartition(partitionID int64) {
}
