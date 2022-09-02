package topicwriter

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
)

type (
	Message      = topicwriterinternal.Message
	Partitioning = topicwriterinternal.PublicPartitioning
)

func NewPartitioningWithMessageGroupID(id string) Partitioning {
	return topicwriterinternal.NewPartitioningWithMessageGroupID(id)
}

func NewPartitioningWithPartitionID(id int64) Partitioning {
	return topicwriterinternal.NewPartitioningWithPartitionID(id)
}

type Writer struct {
	inner *topicwriterinternal.Writer
}

func NewWriter(writer *topicwriterinternal.Writer) *Writer {
	return &Writer{
		inner: writer,
	}
}

func (w *Writer) Write(ctx context.Context, messages ...Message) error {
	return w.inner.Write(ctx, messages...)
}

func (w *Writer) Close(ctx context.Context) error {
	return w.inner.Close(ctx)
}
