package topicmultiwriter

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwritercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
)

type writer interface {
	Close(ctx context.Context) error
	WaitInitInfo(ctx context.Context) (topicwriterinternal.InitialInfo, error)
	WriteInternal(ctx context.Context, messages []topicwritercommon.MessageWithDataContent) error
}

type writersFactory interface {
	Create(cfg topicwriterinternal.WriterReconnectorConfig) (writer, error)
}
