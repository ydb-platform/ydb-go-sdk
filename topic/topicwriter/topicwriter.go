package topicwriter

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
)

type (
	Message      = topicwriterinternal.Message
	Partitioning = topicwriterinternal.PublicPartitioning
)

// Writer represent write session to topic
// It handles connection problems, reconnect to server when need and resend buffered messages
type Writer struct {
	inner *topicwriterinternal.Writer
}

func NewWriter(writer *topicwriterinternal.Writer) *Writer {
	return &Writer{
		inner: writer,
	}
}

// Write send messages to topic
// return fast in async mode (default) and wait ack from server in sync mode.
// see topicoptions.WithSyncWrite
//
// The method will wait first initial connection even for async mode, that mean first write may be slower.
// especially when connection has problems.
//
// ctx cancelation mean cancel of wait ack only, it will not cancel of send messages
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
