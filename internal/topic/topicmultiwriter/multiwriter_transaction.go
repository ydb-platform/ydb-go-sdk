package topicmultiwriter

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// MultiWriterWithTransaction is a transactional wrapper over MultiWriter.
// It mirrors the semantics of topicwriterinternal.WriterWithTransaction:
//   - before commit, it flushes all pending messages by calling Close(ctx)
//   - after transaction completion (commit or rollback), it closes the writer without flush.
type MultiWriterWithTransaction struct {
	multiWriter *MultiWriter
	tx          tx.Transaction
	tracer      *trace.Topic
}

// NewTopicMultiWriterTransaction creates a transactional multi-writer wrapper.
// The underlying MultiWriter must be created with Transaction-aware writers factory
// (see newTransactionalWritersFactory).
func NewTopicMultiWriterTransaction(
	mw *MultiWriter,
	transaction tx.Transaction,
	tracer *trace.Topic,
) *MultiWriterWithTransaction {
	res := &MultiWriterWithTransaction{
		multiWriter: mw,
		tx:          transaction,
	}

	if tracer == nil {
		res.tracer = &trace.Topic{}
	} else {
		res.tracer = tracer
	}

	transaction.OnBeforeCommit(res.onBeforeCommitTransaction)
	transaction.OnCompleted(res.onTransactionCompleted)

	return res
}

func (w *MultiWriterWithTransaction) onBeforeCommitTransaction(ctx context.Context) (err error) {
	// For multi-writer we do not have a single topic session ID like WriterReconnector,
	// so we only ensure that all buffered messages are flushed by closing the writer.
	//
	// Per-writer transactional wrappers (created by transactionalWritersFactory)
	// will handle tracing on their own.
	return w.multiWriter.Close(ctx)
}

func (w *MultiWriterWithTransaction) onTransactionCompleted(err error) {
	// After transaction finished by any reason - the writer is closed without flush.
	noNeedFlushCtx, cancel := context.WithCancel(context.Background())
	cancel()

	_ = w.multiWriter.Close(noNeedFlushCtx)
}

// WaitInit waits until initialization is completed or an error occurs.
func (w *MultiWriterWithTransaction) WaitInit(
	ctx context.Context,
) error {
	return w.multiWriter.WaitInit(ctx)
}

func (w *MultiWriterWithTransaction) WaitInitInfo(ctx context.Context) (topicwriterinternal.InitialInfo, error) {
	return w.multiWriter.WaitInitInfo(ctx)
}

// Write sends messages using the underlying multi-writer.
func (w *MultiWriterWithTransaction) Write(
	ctx context.Context,
	messages []topicwriterinternal.PublicMessage,
) error {
	return w.multiWriter.Write(ctx, messages)
}

// Close gracefully stops writer, flushing pending messages.
func (w *MultiWriterWithTransaction) Close(ctx context.Context) error {
	return w.multiWriter.Close(ctx)
}
