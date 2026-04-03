package topicwriterinternal

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwritercommon"

type PublicMessage = topicwritercommon.PublicMessage

type PublicFuturePartitioning = topicwritercommon.PublicFuturePartitioning

type messageWithDataContent = topicwritercommon.MessageWithDataContent

func NewPartitioningWithMessageGroupID(id string) PublicFuturePartitioning {
	return topicwritercommon.NewPartitioningWithMessageGroupID(id)
}

func NewPartitioningWithPartitionID(id int64) PublicFuturePartitioning {
	return topicwritercommon.NewPartitioningWithPartitionID(id)
}

func newMessageDataWithContent(
	message PublicMessage, //nolint:gocritic
	encoders *topicwritercommon.MultiEncoder,
) messageWithDataContent {
	return topicwritercommon.NewMessageDataWithContent(message, encoders)
}
