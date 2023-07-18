package testutil

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreaderinternal"

type MessageBuilder = topicreaderinternal.PublicMessageBuilder

// NewMessageBuilder create builder, which can create Message (use for tests only)
func NewMessageBuilder() *MessageBuilder {
	return topicreaderinternal.NewPublicMessageBuilder()
}
