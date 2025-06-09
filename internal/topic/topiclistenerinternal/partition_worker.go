package topiclistenerinternal

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
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

// unifiedMessage wraps three types of messages that PartitionWorker can handle
type unifiedMessage struct {
	// Only one of these should be set
	RawServerMessage *rawtopicreader.ServerMessage
	BatchMessage     *batchMessage
}

// batchMessage represents a ready PublicBatch message with metadata
type batchMessage struct {
	ServerMessageMetadata rawtopiccommon.ServerMessageMetadata
	Batch                 *topicreadercommon.PublicBatch
}

// WorkerStoppedCallback notifies when worker is stopped
type WorkerStoppedCallback func(sessionID int64, err error)

// PartitionWorker processes messages for a single partition
type PartitionWorker struct {
	sessionID     int64
	session       *topicreadercommon.PartitionSession
	messageSender MessageSender
	userHandler   EventHandler
	onStopped     WorkerStoppedCallback

	// Tracing fields
	tracer     *trace.Topic
	listenerID string

	messageQueue *xsync.UnboundedChan[unifiedMessage]
	bgWorker     *background.Worker
}

// NewPartitionWorker creates a new PartitionWorker instance
func NewPartitionWorker(
	sessionID int64,
	session *topicreadercommon.PartitionSession,
	messageSender MessageSender,
	userHandler EventHandler,
	onStopped WorkerStoppedCallback,
	tracer *trace.Topic,
	listenerID string,
) *PartitionWorker {
	// TODO: Add trace for worker creation - uncomment when linter issues are resolved
	// logCtx := context.Background()
	// trace.TopicOnPartitionWorkerStart(tracer, &logCtx, listenerID, "", session.StreamPartitionSessionID, session.PartitionID, session.Topic)

	return &PartitionWorker{
		sessionID:     sessionID,
		session:       session,
		messageSender: messageSender,
		userHandler:   userHandler,
		onStopped:     onStopped,
		tracer:        tracer,
		listenerID:    listenerID,
		messageQueue:  xsync.NewUnboundedChan[unifiedMessage](),
	}
}

// Start begins processing messages for this partition
func (w *PartitionWorker) Start(ctx context.Context) {
	w.bgWorker = background.NewWorker(ctx, "partition worker")
	w.bgWorker.Start("partition worker message loop", func(bgCtx context.Context) {
		w.receiveMessagesLoop(bgCtx)
	})
}

// SendMessage adds a unified message to the processing queue
func (w *PartitionWorker) SendMessage(msg unifiedMessage) {
	w.messageQueue.SendWithMerge(msg, w.tryMergeMessages)
}

// SendRawServerMessage sends a raw server message
func (w *PartitionWorker) SendRawServerMessage(msg rawtopicreader.ServerMessage) {
	w.SendMessage(unifiedMessage{RawServerMessage: &msg})
}

// SendBatchMessage sends a ready batch message
func (w *PartitionWorker) SendBatchMessage(metadata rawtopiccommon.ServerMessageMetadata, batch *topicreadercommon.PublicBatch) {
	w.SendMessage(unifiedMessage{
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
	defer func() {
		if r := recover(); r != nil {
			w.onStopped(w.sessionID, xerrors.WithStackTrace(fmt.Errorf("ydb: partition worker panic: %v", r)))
		}
	}()

	for {
		// Use context-aware Receive method
		msg, ok, err := w.messageQueue.Receive(ctx)
		if err != nil {
			// Context was cancelled or timed out
			w.onStopped(w.sessionID, nil) // graceful shutdown
			return
		}
		if !ok {
			// Queue was closed
			w.onStopped(w.sessionID, xerrors.WithStackTrace(errPartitionQueueClosed))
			return
		}

		if err := w.processUnifiedMessage(ctx, msg); err != nil {
			w.onStopped(w.sessionID, err)
			return
		}
	}
}

// processUnifiedMessage handles a single unified message by routing to appropriate processor
func (w *PartitionWorker) processUnifiedMessage(ctx context.Context, msg unifiedMessage) error {
	switch {
	case msg.RawServerMessage != nil:
		return w.processRawServerMessage(ctx, *msg.RawServerMessage)
	case msg.BatchMessage != nil:
		return w.processBatchMessage(ctx, msg.BatchMessage)
	default:
		// Ignore empty messages
		return nil
	}
}

// processRawServerMessage handles raw server messages (StartPartition, StopPartition)
func (w *PartitionWorker) processRawServerMessage(ctx context.Context, msg rawtopicreader.ServerMessage) error {
	switch m := msg.(type) {
	case *rawtopicreader.StartPartitionSessionRequest:
		return w.handleStartPartitionRequest(ctx, m)
	case *rawtopicreader.StopPartitionSessionRequest:
		return w.handleStopPartitionRequest(ctx, m)
	default:
		// Ignore unknown raw message types (e.g., ReadResponse which is now handled via BatchMessage)
		return nil
	}
}

// processBatchMessage handles ready PublicBatch messages
func (w *PartitionWorker) processBatchMessage(ctx context.Context, msg *batchMessage) error {
	// Check for errors in the metadata
	if !msg.ServerMessageMetadata.Status.IsSuccess() {
		return xerrors.WithStackTrace(fmt.Errorf("ydb: batch message contains error status: %v", msg.ServerMessageMetadata.Status))
	}

	// Send ReadRequest for flow control with the batch size
	requestBytesSize := 0
	if msg.Batch != nil {
		for i := range msg.Batch.Messages {
			requestBytesSize += topicreadercommon.MessageGetBufferBytesAccount(msg.Batch.Messages[i])
		}
	}

	// Call user handler with PublicReadMessages if handler is available
	if w.userHandler != nil {
		// Cast messageSender to CommitHandler (it's the streamListener)
		commitHandler, ok := w.messageSender.(CommitHandler)
		if !ok {
			return xerrors.WithStackTrace(fmt.Errorf("ydb: messageSender does not implement CommitHandler"))
		}

		event := NewPublicReadMessages(
			w.session.ToPublic(),
			msg.Batch,
			commitHandler,
		)

		if err := w.userHandler.OnReadMessages(ctx, event); err != nil {
			return xerrors.WithStackTrace(err)
		}
	}

	// Use estimated bytes size for flow control
	w.messageSender.SendRaw(&rawtopicreader.ReadRequest{BytesSize: requestBytesSize})

	return nil
}

// handleStartPartitionRequest processes StartPartitionSessionRequest
func (w *PartitionWorker) handleStartPartitionRequest(ctx context.Context, m *rawtopicreader.StartPartitionSessionRequest) error {
	event := NewPublicStartPartitionSessionEvent(
		w.session.ToPublic(),
		m.CommittedOffset.ToInt64(),
		PublicOffsetsRange{
			Start: m.PartitionOffsets.Start.ToInt64(),
			End:   m.PartitionOffsets.End.ToInt64(),
		},
	)

	if w.userHandler != nil {
		err := w.userHandler.OnStartPartitionSessionRequest(ctx, event)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
	} else {
		// Auto-confirm if no handler
		event.Confirm()
	}

	// Wait for user confirmation
	var userResp PublicStartPartitionSessionConfirm
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-event.confirm.Done():
		userResp, _ = event.confirm.Get()
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
func (w *PartitionWorker) handleStopPartitionRequest(ctx context.Context, m *rawtopicreader.StopPartitionSessionRequest) error {
	event := NewPublicStopPartitionSessionEvent(
		w.session.ToPublic(),
		m.Graceful,
		m.CommittedOffset.ToInt64(),
	)

	if w.userHandler != nil {
		if err := w.userHandler.OnStopPartitionSessionRequest(ctx, event); err != nil {
			return xerrors.WithStackTrace(err)
		}
	} else {
		// Auto-confirm if no handler
		event.Confirm()
	}

	// Wait for user confirmation
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-event.confirm.Done():
		// pass
	}

	// Only send response if graceful
	if m.Graceful {
		resp := &rawtopicreader.StopPartitionSessionResponse{
			PartitionSessionID: w.session.StreamPartitionSessionID,
		}
		w.messageSender.SendRaw(resp)
	}

	return nil
}

// tryMergeMessages attempts to merge messages when possible
func (w *PartitionWorker) tryMergeMessages(last, new unifiedMessage) (unifiedMessage, bool) {
	// Only merge batch messages for now
	if last.BatchMessage != nil && new.BatchMessage != nil {
		// Validate metadata compatibility before merging
		if !last.BatchMessage.ServerMessageMetadata.Equals(&new.BatchMessage.ServerMessageMetadata) {
			return new, false // Don't merge messages with different metadata
		}

		// For batch messages, we don't merge the actual batches since they're already processed
		// Just keep the newer message
		return new, true
	}

	// Don't merge other types of messages
	return new, false
}
