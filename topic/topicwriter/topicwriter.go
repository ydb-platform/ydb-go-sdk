package topicwriter

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
)

type (
	Message = topicwriterinternal.PublicMessage
)

var ErrQueueLimitExceed = topicwriterinternal.PublicErrQueueIsFull

// Writer represent write session to topic
// It handles connection problems, reconnect to server when need and resend buffered messages
type Writer struct {
	inner *topicwriterinternal.Writer
}

// PublicInitialInfo is an information about writer after initialize
type PublicInitialInfo struct {
	LastSeqNum int64
}

// NewWriter create new writer from internal type. Used internally only.
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
func (w *Writer) Write(ctx context.Context, messages ...Message) error {
	return w.inner.Write(ctx, messages...)
}

// WaitInit waits until the reader is initialized
// or an error occurs, return PublicInitialInfo and err
func (w *Writer) WaitInit(ctx context.Context) (err error) {
	_, err = w.inner.WaitInit(ctx)
	if err != nil {
		return err
	}

	return nil
}

// WaitInitInfo waits until the reader is initialized
// or an error occurs, return PublicInitialInfo and err
func (w *Writer) WaitInitInfo(ctx context.Context) (info PublicInitialInfo, err error) {
	privateInfo, err := w.inner.WaitInit(ctx)
	if err != nil {
		return PublicInitialInfo{}, err
	}
	publicInfo := PublicInitialInfo{LastSeqNum: privateInfo.LastSeqNum}

	return publicInfo, nil
}

// Close will flush rested messages from buffer and close the writer.
// You can't write new messages after call Close
func (w *Writer) Close(ctx context.Context) error {
	return w.inner.Close(ctx)
}

// Flush waits till all in-flight messages are acknowledged.
func (w *Writer) Flush(ctx context.Context) error {
	return w.inner.Flush(ctx)
}
