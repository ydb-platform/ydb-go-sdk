package topicpartitions

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

type PartitionChooser interface {
	ChoosePartition(msg topicwriterinternal.PublicMessage) (int64, error)
	AddNewPartitions(partitions ...topictypes.PartitionInfo) error
	RemovePartition(partitionID int64)
}
