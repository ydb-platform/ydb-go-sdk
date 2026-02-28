package topicproducer

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
)

type writer interface {
	Close(ctx context.Context) error
	WaitInit(ctx context.Context) error
	Write(ctx context.Context, messages ...topicwriterinternal.PublicMessage) error
}

type writersFactory interface {
	Create(topicPath string, opts ...topicwriterinternal.PublicWriterOption) (writer, error)
}
