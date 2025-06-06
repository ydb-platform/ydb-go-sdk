package topiclistenerinternal

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

//go:generate mockgen -source partition_worker.go -destination partition_worker_mock_test.go --typed -package topiclistenerinternal -write_package_comment=false

var errPartitionQueueClosed = xerrors.Wrap(fmt.Errorf("ydb: partition messages queue closed"))

// MessageSender sends messages back to server
type MessageSender interface {
	SendRaw(msg rawtopicreader.ClientMessage)
}

// WorkerStoppedCallback notifies when worker is stopped
type WorkerStoppedCallback func(sessionID int64, err error)

// PartitionWorker processes messages for a single partition
type PartitionWorker struct {
	sessionID      int64
	session        *topicreadercommon.PartitionSession
	messageSender  MessageSender
	userHandler    EventHandler
	onStopped      WorkerStoppedCallback
	streamListener *streamListener

	queue    *xsync.UnboundedChan[rawtopicreader.ServerMessage]
	bgWorker *background.Worker
}

// NewPartitionWorker creates a new PartitionWorker instance
func NewPartitionWorker(
	sessionID int64,
	session *topicreadercommon.PartitionSession,
	messageSender MessageSender,
	userHandler EventHandler,
	onStopped WorkerStoppedCallback,
	streamListener *streamListener,
) *PartitionWorker {
	return &PartitionWorker{
		sessionID:      sessionID,
		session:        session,
		messageSender:  messageSender,
		userHandler:    userHandler,
		onStopped:      onStopped,
		streamListener: streamListener,
		queue:          xsync.NewUnboundedChan[rawtopicreader.ServerMessage](),
	}
}

// Start begins processing messages for this partition
func (w *PartitionWorker) Start(ctx context.Context) {
	w.bgWorker = background.NewWorker(ctx, "partition worker")
	w.bgWorker.Start("partition worker message loop", w.receiveMessagesLoop)
}

// SendMessage adds a message to the processing queue
func (w *PartitionWorker) SendMessage(msg rawtopicreader.ServerMessage) {
	w.queue.SendWithMerge(msg, w.tryMergeMessages)
}

// Close stops the worker gracefully
func (w *PartitionWorker) Close(ctx context.Context, reason error) error {
	w.queue.Close()
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
		msg, ok, err := w.queue.Receive(ctx)
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

		if err := w.processMessage(ctx, msg); err != nil {
			w.onStopped(w.sessionID, err)
			return
		}
	}
}

// processMessage handles a single server message
func (w *PartitionWorker) processMessage(ctx context.Context, msg rawtopicreader.ServerMessage) error {
	switch m := msg.(type) {
	case *rawtopicreader.StartPartitionSessionRequest:
		return w.handleStartPartitionRequest(ctx, m)
	case *rawtopicreader.StopPartitionSessionRequest:
		return w.handleStopPartitionRequest(ctx, m)
	case *rawtopicreader.ReadResponse:
		return w.handleReadResponse(ctx, m)
	default:
		// Ignore unknown message types
		return nil
	}
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

	err := w.userHandler.OnStartPartitionSessionRequest(ctx, event)
	if err != nil {
		return err
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

	if err := w.userHandler.OnStopPartitionSessionRequest(ctx, event); err != nil {
		return err
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

// handleReadResponse processes ReadResponse messages
func (w *PartitionWorker) handleReadResponse(ctx context.Context, m *rawtopicreader.ReadResponse) error {
	// Convert raw batches to public batches - following streamListener pattern
	sessions := &topicreadercommon.PartitionSessionStorage{}
	if err := sessions.Add(w.session); err != nil {
		return err
	}

	batches, err := topicreadercommon.ReadRawBatchesToPublicBatches(m, sessions, topicreadercommon.NewDecoderMap())
	if err != nil {
		return err
	}

	// Process each batch individually
	for _, batch := range batches {
		event := NewPublicReadMessages(
			topicreadercommon.BatchGetPartitionSession(batch).ToPublic(),
			batch,
			w.streamListener,
		)

		if err := w.userHandler.OnReadMessages(ctx, event); err != nil {
			return err
		}
	}

	return nil
}

// tryMergeMessages attempts to merge ReadResponse messages with metadata validation
func (w *PartitionWorker) tryMergeMessages(last, new rawtopicreader.ServerMessage) (rawtopicreader.ServerMessage, bool) {
	lastRead, lastOk := last.(*rawtopicreader.ReadResponse)
	newRead, newOk := new.(*rawtopicreader.ReadResponse)

	if !lastOk || !newOk {
		return new, false // Only merge ReadResponse messages
	}

	// Validate metadata compatibility before merging
	if !lastRead.ServerMessageMetadata.Equals(&newRead.ServerMessageMetadata) {
		return new, false // Don't merge messages with different metadata
	}

	// Merge by combining message batches
	merged := &rawtopicreader.ReadResponse{
		PartitionData:         append(lastRead.PartitionData, newRead.PartitionData...),
		BytesSize:             lastRead.BytesSize + newRead.BytesSize,
		ServerMessageMetadata: newRead.ServerMessageMetadata, // Use metadata from newer message
	}

	return merged, true
}
