package topicmultiwriter

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
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
