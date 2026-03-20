package partitionchooser

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

type byPartitionIDPartitionChooser struct{}

func NewByPartitionIDPartitionChooser() *byPartitionIDPartitionChooser {
	return &byPartitionIDPartitionChooser{}
}

func (c *byPartitionIDPartitionChooser) ChoosePartition(msg topicwriterinternal.PublicMessage) (int64, error) {
	return msg.PartitionID, nil
}

func (c *byPartitionIDPartitionChooser) AddNewPartitions(partitions ...topictypes.PartitionInfo) error {
	return nil
}

func (c *byPartitionIDPartitionChooser) RemovePartition(partitionID int64) {
}
