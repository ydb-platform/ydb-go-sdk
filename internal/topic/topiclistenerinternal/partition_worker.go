package topiclistenerinternal

import (
	"context"
	"errors"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

//go:generate mockgen -source partition_worker.go -destination partition_worker_mock_test.go --typed -package topiclistenerinternal -write_package_comment=false

var errPartitionQueueClosed = xerrors.Wrap(fmt.Errorf("ydb: partition messages queue closed"))

// MessageSender sends messages back to server
type MessageSender interface {
	SendRaw(msg rawtopicreader.ClientMessage)
}

// ReadBufferReleaser returns read-ahead buffer credit.
type ReadBufferReleaser interface {
	ReadBufferRelease(size int)
}

// unifiedMessage wraps messages that PartitionWorker can handle
type unifiedMessage struct {
	PartitionSession *topicreadercommon.PartitionSession
	// Only one of these should be set
	RawServerMessage *rawtopicreader.ServerMessage
	BatchMessage     *batchMessage
}

func (m unifiedMessage) partitionSession() *topicreadercommon.PartitionSession {
	if m.BatchMessage != nil {
		return topicreadercommon.BatchGetPartitionSession(m.BatchMessage.Batch)
	}

	return m.PartitionSession
}

// batchMessage represents a ready PublicBatch message with metadata
type batchMessage struct {
	ServerMessageMetadata rawtopiccommon.ServerMessageMetadata
	Batch                 *topicreadercommon.PublicBatch
}

func batchReadBufferSize(batch *topicreadercommon.PublicBatch) int {
	if batch == nil {
		return 0
	}

	size := 0
	for i := range batch.Messages {
		size += topicreadercommon.MessageGetBufferBytesAccount(batch.Messages[i])
	}

	return size
}

// WorkerStoppedCallback reports why a worker stopped and its last processed session.
type WorkerStoppedCallback func(sessionID rawtopicreader.PartitionSessionID, reason error)

// PartitionWorker processes messages for one topic partition across partition sessions.
type PartitionWorker struct {
	partitionSessionID rawtopicreader.PartitionSessionID
	partitionSession   *topicreadercommon.PartitionSession
	messageSender      interface {
		MessageSender
		CommitHandler
	}
	readBufferReleaser ReadBufferReleaser
	onLocalBufferDelta func(delta int) bool
	userHandler        EventHandler
	onStopped          WorkerStoppedCallback

	// Tracing and logging fields
	tracer     *trace.Topic
	listenerID string
	readerInfo topicreadercommon.ReaderInfo

	messageQueue *xsync.UnboundedChan[unifiedMessage]
	bgWorker     *background.Worker
}

// NewPartitionWorker creates a new PartitionWorker instance
func NewPartitionWorker[T interface {
	MessageSender
	ReadBufferReleaser
	CommitHandler
}](
	sessionID rawtopicreader.PartitionSessionID,
	session *topicreadercommon.PartitionSession,
	messageSender T,
	userHandler EventHandler,
	onStopped WorkerStoppedCallback,
	tracer *trace.Topic,
	listenerID string,
) *PartitionWorker {
	// Validate required parameters
	if userHandler == nil {
		panic("userHandler cannot be nil")
	}

	return &PartitionWorker{
		partitionSessionID: sessionID,
		partitionSession:   session,
		messageSender:      messageSender,
		readBufferReleaser: messageSender,
		userHandler:        userHandler,
		onStopped:          onStopped,
		tracer:             tracer,
		listenerID:         listenerID,
		messageQueue:       xsync.NewUnboundedChan[unifiedMessage](),
	}
}

// Start begins processing messages for this partition
func (w *PartitionWorker) Start(ctx context.Context) {
	// Add trace for worker creation
	gtrace.TopicOnPartitionWorkerStart(
		w.tracer,
		&ctx,
		w.listenerID,
		"", // sessionID - not available in worker context
		int64(w.partitionSessionID),
		w.partitionSession.PartitionID,
		w.partitionSession.Topic,
	)

	w.bgWorker = background.NewWorker(ctx, "partition worker")
	context.AfterFunc(w.bgWorker.Context(), w.partitionSession.Close)
	w.bgWorker.Start("partition worker message loop", func(bgCtx context.Context) {
		w.receiveMessagesLoop(bgCtx)
	})
}

// AddUnifiedMessage adds a unified message to the processing queue
func (w *PartitionWorker) AddUnifiedMessage(msg unifiedMessage) bool {
	var localBufferMessages int
	if msg.BatchMessage != nil && msg.BatchMessage.Batch != nil {
		localBufferMessages = len(msg.BatchMessage.Batch.Messages)
	}

	reserved := false
	if msg.BatchMessage != nil {
		reserved = w.changeLocalBuffer(localBufferMessages)
	}

	accepted := w.messageQueue.SendWithMerge(msg, func(last, next unifiedMessage) (unifiedMessage, bool) {
		if last.BatchMessage == nil || next.BatchMessage == nil ||
			!last.BatchMessage.ServerMessageMetadata.Equals(&next.BatchMessage.ServerMessageMetadata) {
			return next, false
		}

		merged, err := topicreadercommon.BatchAppend(last.BatchMessage.Batch, next.BatchMessage.Batch)
		if err != nil {
			return next, false
		}

		return unifiedMessage{BatchMessage: &batchMessage{
			ServerMessageMetadata: last.BatchMessage.ServerMessageMetadata,
			Batch:                 merged,
		}}, true
	})
	if !accepted {
		if reserved {
			w.changeLocalBuffer(-localBufferMessages)
		}
		w.freeBatchCredit(msg)
	}

	return accepted
}

// AddRawServerMessage sends a raw server message
func (w *PartitionWorker) AddRawServerMessage(
	session *topicreadercommon.PartitionSession,
	msg rawtopicreader.ServerMessage,
) {
	w.AddUnifiedMessage(unifiedMessage{PartitionSession: session, RawServerMessage: &msg})
}

// AddMessagesBatch sends a ready batch message
func (w *PartitionWorker) AddMessagesBatch(
	metadata rawtopiccommon.ServerMessageMetadata,
	batch *topicreadercommon.PublicBatch,
) bool {
	return w.AddUnifiedMessage(unifiedMessage{
		BatchMessage: &batchMessage{
			ServerMessageMetadata: metadata,
			Batch:                 batch,
		},
	})
}

// Close stops the worker gracefully
func (w *PartitionWorker) Close(ctx context.Context, reason error) error {
	w.messageQueue.Close()
	if w.bgWorker != nil {
		return w.bgWorker.Close(ctx, reason)
	}

	return nil
}

// receiveMessagesLoop is the main message processing loop
func (w *PartitionWorker) receiveMessagesLoop(ctx context.Context) {
	session := w.partitionSession
	sessionID := w.partitionSessionID
	defer func() {
		if r := recover(); r != nil {
			reason := xerrors.WithStackTrace(fmt.Errorf("ydb: partition worker panic: %v", r))
			w.onStopped(sessionID, reason)
		}
	}()

	// Close before the final drain so a concurrent sender either queues before the
	// drain or observes the closed queue and releases its own buffer credit.
	defer w.closeQueueAndFreeBufferedBatchCredits()

	for {
		// Use context-aware Receive method
		msg, ok, err := w.messageQueue.Receive(ctx)
		if err != nil {
			// Context was cancelled or timed out
			if ctx.Err() == context.Canceled {
				reason := xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf(
					"graceful shutdown PartitionWorker: topic=%s, partition=%d, partitionSession=%d",
					session.Topic,
					session.PartitionID,
					sessionID,
				)))
				w.onStopped(sessionID, reason)
			} else {
				reason := xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf(
					"partition worker message queue context error: topic=%s, partition=%d, partitionSession=%d: %w",
					session.Topic,
					session.PartitionID,
					sessionID,
					err,
				)))
				w.onStopped(sessionID, reason)
			}

			return
		}
		if !ok {
			// Queue was closed - this is a normal stop reason during shutdown
			reason := xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf(
				"partition worker message queue closed: topic=%s, partition=%d, partitionSession=%d: %w",
				session.Topic,
				session.PartitionID,
				sessionID,
				errPartitionQueueClosed,
			)))
			w.onStopped(sessionID, reason)

			return
		}

		if messageSession := msg.partitionSession(); messageSession != nil {
			session = messageSession
			sessionID = session.StreamPartitionSessionID
		}
		if err := w.processUnifiedMessage(ctx, msg); err != nil {
			w.onStopped(sessionID, err)

			return
		}
	}
}

// processUnifiedMessage handles a single unified message by routing to appropriate processor
func (w *PartitionWorker) processUnifiedMessage(ctx context.Context, msg unifiedMessage) error {
	if err := ctx.Err(); err != nil {
		w.releaseQueuedBatch(msg)

		return err
	}

	session := msg.partitionSession()
	if session == nil {
		return nil
	}
	callbackCtx := xcontext.MergeContexts(session.Context(), ctx)

	var err error
	switch {
	case msg.RawServerMessage != nil:
		err = w.processRawServerMessage(callbackCtx, session, *msg.RawServerMessage)
	case msg.BatchMessage != nil:
		select {
		case <-session.Context().Done():
			w.releaseQueuedBatch(msg)

			return nil
		default:
		}
		err = w.processBatchMessage(callbackCtx, msg.BatchMessage)
	}
	// Revocation ends this session, not the partition worker or the stream.
	if session.Context().Err() != nil && (errors.Is(err, context.Canceled) ||
		errors.Is(err, topicreadercommon.ErrPublicCommitSessionToExpiredSession)) {
		return nil
	}

	return err
}

// processRawServerMessage handles raw server messages (StartPartition, StopPartition)
func (w *PartitionWorker) processRawServerMessage(
	ctx context.Context,
	session *topicreadercommon.PartitionSession,
	msg rawtopicreader.ServerMessage,
) error {
	switch m := msg.(type) {
	case *rawtopicreader.StartPartitionSessionRequest:
		return w.handleStartPartitionRequest(ctx, session, m)
	case *rawtopicreader.StopPartitionSessionRequest:
		return w.handleStopPartitionRequest(ctx, session, m)
	default:
		// Ignore unknown raw message types (e.g., ReadResponse which is now handled via BatchMessage)
		return nil
	}
}

// validateBatchMetadata checks if the batch metadata status is successful
func (w *PartitionWorker) validateBatchMetadata(msg *batchMessage) error {
	if !msg.ServerMessageMetadata.Status.IsSuccess() {
		err := xerrors.WithStackTrace(fmt.Errorf(
			"ydb: batch message contains error status: %v",
			msg.ServerMessageMetadata.Status,
		))

		return err
	}

	return nil
}

// callUserHandler calls the user handler for a batch message with tracing
func (w *PartitionWorker) callUserHandler(
	ctx context.Context,
	msg *batchMessage,
	commitHandler CommitHandler,
	messagesCount int,
) error {
	session := topicreadercommon.BatchGetPartitionSession(msg.Batch)
	event := NewPublicReadMessages(
		session.ToPublic(),
		msg.Batch,
		commitHandler,
	)

	// Add handler call tracing
	handlerTraceDone := gtrace.TopicOnPartitionWorkerHandlerCall(
		w.tracer,
		&ctx,
		w.listenerID,
		"", // sessionID - not available in worker context
		int64(session.StreamPartitionSessionID),
		session.PartitionID,
		session.Topic,
		"OnReadMessages",
		messagesCount,
	)
	if msg.Batch != nil && messagesCount > 0 {
		traceCtx := ctx
		gtrace.TopicOnReaderMessagesDelivered(
			w.tracer,
			&traceCtx,
			w.readerInfo,
			session.Topic,
			messagesCount,
		)
	}

	if err := w.userHandler.OnReadMessages(ctx, event); err != nil {
		handlerErr := xerrors.WithStackTrace(err)
		handlerTraceDone(handlerErr)

		return handlerErr
	}

	handlerTraceDone(nil)

	return nil
}

// processBatchMessage handles ready PublicBatch messages
func (w *PartitionWorker) processBatchMessage(ctx context.Context, msg *batchMessage) error {
	session := topicreadercommon.BatchGetPartitionSession(msg.Batch)
	// Add tracing for batch processing
	messagesCount := 0
	if msg.Batch != nil {
		messagesCount = len(msg.Batch.Messages)
	}

	traceDone := gtrace.TopicOnPartitionWorkerProcessMessage(
		w.tracer,
		&ctx,
		w.listenerID,
		"", // sessionID - not available in worker context
		int64(session.StreamPartitionSessionID),
		session.PartitionID,
		session.Topic,
		"BatchMessage",
		messagesCount,
	)
	// Release buffer credit when the batch leaves the worker (success, validation error,
	// or handler error). defer — not after handler only — so credits are not leaked.
	// Credit is tied to read/processing, not commit (protocol separates these; same as reader).
	batchSize := batchReadBufferSize(msg.Batch)
	defer w.readBufferReleaser.ReadBufferRelease(batchSize)
	w.changeLocalBuffer(-messagesCount)

	// Check for errors in the metadata
	if err := w.validateBatchMetadata(msg); err != nil {
		traceDone(0, err)

		return err
	}

	// Call user handler with tracing
	if err := w.callUserHandler(ctx, msg, w.messageSender, messagesCount); err != nil {
		traceDone(0, err)

		return err
	}

	traceDone(messagesCount, nil)

	return nil
}

// freeBufferedBatchCredits returns buffer credit for batches still buffered in the
// partition queue. In-flight batch is freed by processBatchMessage defer, not here.
func (w *PartitionWorker) freeBufferedBatchCredits() {
	for _, msg := range w.messageQueue.DrainBuffered() {
		w.releaseQueuedBatch(msg)
	}
}

func (w *PartitionWorker) releaseQueuedBatch(msg unifiedMessage) {
	if msg.BatchMessage == nil {
		return
	}
	batch := msg.BatchMessage.Batch
	if batch != nil {
		w.changeLocalBuffer(-len(batch.Messages))
	}
	w.freeBatchCredit(msg)
}

func (w *PartitionWorker) closeQueueAndFreeBufferedBatchCredits() {
	w.messageQueue.Close()
	w.freeBufferedBatchCredits()
}

func (w *PartitionWorker) freeBatchCredit(msg unifiedMessage) {
	// StartPartition / StopPartition do not consume read-ahead data bytes.
	if msg.BatchMessage != nil {
		w.readBufferReleaser.ReadBufferRelease(batchReadBufferSize(msg.BatchMessage.Batch))
	}
}

func (w *PartitionWorker) changeLocalBuffer(delta int) bool {
	return delta != 0 && w.onLocalBufferDelta != nil && w.onLocalBufferDelta(delta)
}

// handleStartPartitionRequest processes StartPartitionSessionRequest
func (w *PartitionWorker) handleStartPartitionRequest(
	ctx context.Context,
	session *topicreadercommon.PartitionSession,
	m *rawtopicreader.StartPartitionSessionRequest,
) error {
	event := NewPublicStartPartitionSessionEvent(
		session.ToPublic(),
		m.CommittedOffset.ToInt64(),
		PublicOffsetsRange{
			Start: m.PartitionOffsets.Start.ToInt64(),
			End:   m.PartitionOffsets.End.ToInt64(),
		},
	)

	err := w.userHandler.OnStartPartitionSessionRequest(ctx, event)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	// Wait for user confirmation
	var userResp PublicStartPartitionSessionConfirm
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-event.confirm.Done():
		userResp, _ = event.confirm.Get()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// Build response
	resp := &rawtopicreader.StartPartitionSessionResponse{
		PartitionSessionID: m.PartitionSession.PartitionSessionID,
	}

	if userResp.readOffset != nil {
		resp.ReadOffset.Offset.FromInt64(*userResp.readOffset)
		resp.ReadOffset.HasValue = true
	}
	if userResp.CommitOffset != nil {
		resp.CommitOffset.Offset.FromInt64(*userResp.CommitOffset)
		resp.CommitOffset.HasValue = true
	}

	w.messageSender.SendRaw(resp)

	return nil
}

// handleStopPartitionRequest processes StopPartitionSessionRequest
func (w *PartitionWorker) handleStopPartitionRequest(
	ctx context.Context,
	session *topicreadercommon.PartitionSession,
	m *rawtopicreader.StopPartitionSessionRequest,
) error {
	event := NewPublicStopPartitionSessionEvent(
		session.ToPublic(),
		m.Graceful,
		m.CommittedOffset.ToInt64(),
	)

	if err := w.userHandler.OnStopPartitionSessionRequest(ctx, event); err != nil {
		return xerrors.WithStackTrace(err)
	}
	if !m.Graceful {
		return nil
	}

	// Wait for user confirmation
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-event.confirm.Done():
		// pass
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := w.messageSender.flushCommits(); err != nil {
		return xerrors.WithStackTrace(fmt.Errorf(
			"ydb: failed to flush commits before stopping partition session: %w", err,
		))
	}
	session.Close()
	w.messageSender.SendRaw(&rawtopicreader.StopPartitionSessionResponse{
		PartitionSessionID: session.StreamPartitionSessionID,
	})

	return nil
}
