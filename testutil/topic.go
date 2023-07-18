package testutil

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreaderinternal"

type TopicReaderMessageBuilder = topicreaderinternal.PublicMessageBuilder

// NewTopicReaderMessageBuilder create builder, which can create Message (use for tests only)
func NewTopicReaderMessageBuilder() *TopicReaderMessageBuilder {
	return topicreaderinternal.NewPublicMessageBuilder()
}
