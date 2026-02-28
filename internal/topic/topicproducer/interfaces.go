package topicproducer

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
)

type subWriter interface {
	Close(ctx context.Context) error
	WaitInit(ctx context.Context) error
	Write(ctx context.Context, messages ...topicwriterinternal.PublicMessage) error
}

type subWritersFactory interface {
	Create(topicPath string, opts ...topicwriterinternal.PublicWriterOption) (subWriter, error)
}
