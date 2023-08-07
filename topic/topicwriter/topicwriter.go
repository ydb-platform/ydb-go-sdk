package topicwriter

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
)

type (
	Message = topicwriterinternal.Message
)

var ErrQueueLimitExceed = topicwriterinternal.PublicErrQueueIsFull

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
// return after save messages into buffer in async mode (default) and after ack from server in sync mode.
// see topicoptions.WithSyncWrite
//
// The method will wait first initial connection even for async mode, that mean first write may be slower.
// especially when connection has problems.
//
// It returns ErrQueueLimitExceed (must be checked by errors.Is)
// if ctx cancelled before messages put to internal buffer or try to add more messages, that can be put to queue
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func (w *Writer) Write(ctx context.Context, messages ...Message) error {
	return w.inner.Write(ctx, messages...)
}

func (w *Writer) WaitInit(ctx context.Context) (lastSegNo int64, err error) {
	return w.inner.WaitInit(ctx)
}

func (w *Writer) Close(ctx context.Context) error {
	return w.inner.Close(ctx)
}
