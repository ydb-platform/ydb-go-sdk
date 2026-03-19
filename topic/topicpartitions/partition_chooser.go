package topicpartitions

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

type Message topicwriterinternal.PublicMessage

type PartitionChooser interface {
	ChoosePartition(msg Message) (int64, error)
	AddNewPartitions(partitions ...topictypes.PartitionInfo) error
	RemovePartition(partitionID int64)
}
