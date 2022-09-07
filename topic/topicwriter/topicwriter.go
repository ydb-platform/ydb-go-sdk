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

// Write
// Will fast in async mode (return after write message to internal buffer) and wait ack from server in sync mode.
// The method will wait first initial connection even for async mode, that mean first write may be slow.
// especially when connection has problems.
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func (w *Writer) Write(ctx context.Context, messages ...Message) error {
	return w.inner.Write(ctx, messages...)
}

func (w *Writer) Close(ctx context.Context) error {
	return w.inner.Close(ctx)
}
