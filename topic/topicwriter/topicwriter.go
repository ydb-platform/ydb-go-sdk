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
	panic("not implemented")
}

func (w *Writer) Close(ctx context.Context) error {
	panic("not implemented")
}
