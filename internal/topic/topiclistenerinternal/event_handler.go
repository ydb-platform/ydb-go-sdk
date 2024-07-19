package topiclistenerinternal

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
)

type EventHandler interface {
	OnReadMessages(ctx context.Context, req PublicReadMessages) error
}

type PublicReadMessages struct {
	PartitionSessionID int64
	Batch              *topicreader.Batch
}
