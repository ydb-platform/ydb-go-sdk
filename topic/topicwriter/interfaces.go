package topicwriter

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
)

type innerWriter interface {
	Write(ctx context.Context, messages []topicwriterinternal.PublicMessage) error
	Close(ctx context.Context) error
	Flush(ctx context.Context) error
	WaitInit(ctx context.Context) error
	WaitInitInfo(ctx context.Context) (topicwriterinternal.InitialInfo, error)
}

type innerTxWriter interface {
	Write(ctx context.Context, messages []topicwriterinternal.PublicMessage) error
	Close(ctx context.Context) error
	WaitInit(ctx context.Context) error
	WaitInitInfo(ctx context.Context) (topicwriterinternal.InitialInfo, error)
}
