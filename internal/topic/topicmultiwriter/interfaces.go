package topicmultiwriter

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

type writer interface {
	Close(ctx context.Context) error
	WaitInitInfo(ctx context.Context) (topicwriterinternal.InitialInfo, error)
	Write(ctx context.Context, messages []topicwriterinternal.PublicMessage) error
	GetBufferedMessages() []topicwriterinternal.PublicMessage
}

type writersFactory interface {
	Create(cfg topicwriterinternal.WriterReconnectorConfig) (writer, error)
}

type PartitionChooser interface {
	ChoosePartition(msg topicwriterinternal.PublicMessage) (int64, error)
	AddNewPartitions(partitions ...topictypes.PartitionInfo) error
	RemovePartition(partitionID int64)
}
