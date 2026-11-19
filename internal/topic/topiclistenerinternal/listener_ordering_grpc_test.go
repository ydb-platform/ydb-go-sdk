package topiclistenerinternal_test

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/rekby/fixenv"
	"github.com/rekby/fixenv/sf"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Topic_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmock"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topiclistener"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
)

func TestTopicListenerForcedStopFinishesOldSessionBeforeStartingNewSession(t *testing.T) {
	for _, tt := range []struct {
		name                string
		returnCanceledError bool
	}{
		{name: "nil callback error"},
		{name: "canceled callback error", returnCanceledError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			testTopicListenerForcedStop(t, tt.returnCanceledError, nil)
		})
	}
}

func TestTopicListenerBlockedReadDoesNotBlockOtherPartitions(t *testing.T) {
	for _, tt := range []struct {
		name      string
		topic     string
		partition int64
	}{
		{name: "another partition in the same topic", topic: "test-topic", partition: 8},
		{name: "the same partition number in another topic", topic: "another-topic", partition: 7},
	} {
		t.Run(tt.name, func(t *testing.T) {
			testTopicListenerForcedStop(t, false, &orderingPartition{topic: tt.topic, partition: tt.partition})
		})
	}
}

func TestTopicListenerGracefulStopDrainsOldSessionBeforeStartingNewSession(t *testing.T) {
	e := fixenv.New(t)
	releaseRead := make(chan struct{})
	handler := &orderingListenerHandler{
		events:      make(chan orderingListenerEvent, 16),
		readStarted: make(chan struct{}),
		releaseRead: releaseRead,
	}
	server := &orderingTopicServer{
		readStarted:  handler.readStarted,
		stopGraceful: true,
		stopSent:     make(chan struct{}),
		errors:       make(chan error, 1),
	}
	db, err := ydb.Open(sf.Context(e), topicmock.GrpcMockTopicConnString(e, server))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close(context.Background()) })
	listener, err := db.Topic().StartListener("test-consumer", handler, topicoptions.ReadTopic("test-topic"),
		topicoptions.WithListenerCheckRetryErrorFunction(
			func(topicoptions.CheckErrorRetryArgs) topicoptions.CheckErrorRetryResult {
				return topicoptions.CheckErrorRetryDecisionStop
			},
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close(context.Background()) })

	firstStart := receiveOrderingListenerEvent(t, handler.events, server.errors)
	require.Equal(t, "start", firstStart.kind)
	firstRead := receiveOrderingListenerEvent(t, handler.events, server.errors)
	require.Equal(t, "read", firstRead.kind)
	require.NoError(t, firstRead.err)
	require.Equal(t, "in-flight", firstRead.data)
	select {
	case <-server.stopSent:
	case err := <-server.errors:
		t.Fatalf("topic server failed: %v", err)
	}
	close(releaseRead)
	finished := receiveOrderingListenerEvent(t, handler.events, server.errors)
	require.Equal(t, "finished", finished.kind, "graceful Stop must let the active callback finish")
	require.NoError(t, finished.err)
	buffered := receiveOrderingListenerEvent(t, handler.events, server.errors)
	require.Equal(t, "read", buffered.kind)
	require.Equal(t, "buffered", buffered.data)
	require.Equal(t, firstStart.session, buffered.session)
	require.NoError(t, buffered.err)
	stopped := receiveOrderingListenerEvent(t, handler.events, server.errors)
	require.Equal(t, "stop", stopped.kind)
	require.Equal(t, firstStart.session, stopped.session)
	require.NoError(t, stopped.err)
	secondStart := receiveOrderingListenerEvent(t, handler.events, server.errors)
	require.Equal(t, "start", secondStart.kind)
	require.NotEqual(t, firstStart.session, secondStart.session)
	require.EqualValues(t, 2, secondStart.offset)
	secondRead := receiveOrderingListenerEvent(t, handler.events, server.errors)
	require.Equal(t, "read", secondRead.kind)
	require.Equal(t, secondStart.session, secondRead.session)
	require.Equal(t, "new-session", secondRead.data)
	require.NoError(t, secondRead.err)
	committed := receiveOrderingListenerEvent(t, handler.events, server.errors)
	require.Equal(t, "committed", committed.kind)
	require.Equal(t, secondStart.session, committed.session)
	require.NoError(t, committed.err)
}

func testTopicListenerForcedStop(t *testing.T, returnCanceledError bool, independent *orderingPartition) {
	t.Helper()

	e := fixenv.New(t)
	handler := &orderingListenerHandler{
		events:              make(chan orderingListenerEvent, 16),
		readStarted:         make(chan struct{}),
		returnCanceledError: returnCanceledError,
		independentRead:     make(chan struct{}),
	}
	server := &orderingTopicServer{
		readStarted:     handler.readStarted,
		independent:     independent,
		independentRead: handler.independentRead,
		errors:          make(chan error, 1),
	}
	db, err := ydb.Open(sf.Context(e), topicmock.GrpcMockTopicConnString(e, server))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close(context.Background()) })
	selectors := topicoptions.ReadTopic("test-topic")
	if independent != nil && independent.topic != "test-topic" {
		selectors = append(selectors, topicoptions.ReadTopic(independent.topic)...)
	}
	listener, err := db.Topic().StartListener("test-consumer", handler, selectors,
		topicoptions.WithListenerCheckRetryErrorFunction(
			func(topicoptions.CheckErrorRetryArgs) topicoptions.CheckErrorRetryResult {
				return topicoptions.CheckErrorRetryDecisionStop
			},
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close(context.Background()) })

	firstStart := receiveOrderingListenerEvent(t, handler.events, server.errors)
	require.Equal(t, "start", firstStart.kind)
	firstRead := receiveOrderingListenerEvent(t, handler.events, server.errors)
	require.Equal(t, "read", firstRead.kind)
	require.NoError(t, firstRead.err)
	require.Equal(t, "in-flight", firstRead.data)
	require.Equal(t, firstStart.session, firstRead.session)
	if independent != nil {
		independentStart := receiveOrderingListenerEvent(t, handler.events, server.errors)
		require.Equal(t, "start", independentStart.kind)
		require.Equal(t, independent.topic, independentStart.topic)
		require.Equal(t, independent.partition, independentStart.partition)
		independentRead := receiveOrderingListenerEvent(t, handler.events, server.errors)
		require.Equal(t, "read", independentRead.kind,
			"another partition must keep reading while the first partition callback is blocked")
		require.Equal(t, independentStart.session, independentRead.session)
		require.Equal(t, "independent", independentRead.data)
		require.NoError(t, independentRead.err)
	}

	canceled := receiveOrderingListenerEvent(t, handler.events, server.errors)
	require.Equal(t, "canceled", canceled.kind,
		"the old read callback must finish before a new session starts")
	require.Equal(t, firstStart.session, canceled.session)
	require.ErrorIs(t, canceled.err, context.Canceled)
	stopped := receiveOrderingListenerEvent(t, handler.events, server.errors)
	require.Equal(t, "stop", stopped.kind,
		"buffered messages from the forced-stopped session must not reach the handler")
	require.Equal(t, firstStart.session, stopped.session)
	secondStart := receiveOrderingListenerEvent(t, handler.events, server.errors)
	require.Equal(t, "start", secondStart.kind)
	require.NotEqual(t, firstStart.session, secondStart.session)
	require.Zero(t, secondStart.offset)
	for offset, data := range []string{"in-flight", "buffered", "new-session"} {
		replayed := receiveOrderingListenerEvent(t, handler.events, server.errors)
		require.Equal(t, "read", replayed.kind)
		require.Equal(t, secondStart.session, replayed.session)
		require.Equal(t, data, replayed.data)
		require.EqualValues(t, offset, replayed.offset)
		require.NoError(t, replayed.err)
		committed := receiveOrderingListenerEvent(t, handler.events, server.errors)
		require.Equal(t, "committed", committed.kind)
		require.Equal(t, secondStart.session, committed.session)
		require.NoError(t, committed.err)
	}
}

type orderingListenerEvent struct {
	kind      string
	session   int64
	topic     string
	partition int64
	data      string
	offset    int64
	err       error
}

type orderingPartition struct {
	topic     string
	partition int64
}

func receiveOrderingListenerEvent(
	t *testing.T,
	events <-chan orderingListenerEvent,
	serverErrors <-chan error,
) orderingListenerEvent {
	t.Helper()

	select {
	case event := <-events:
		return event
	case err := <-serverErrors:
		t.Fatalf("topic server failed: %v", err)

		return orderingListenerEvent{}
	}
}

type orderingListenerHandler struct {
	topiclistener.BaseHandler

	events              chan orderingListenerEvent
	readStarted         chan struct{}
	returnCanceledError bool
	independentRead     chan struct{}
	releaseRead         <-chan struct{}
	firstSession        atomic.Int64
	firstSessionOnce    sync.Once
}

func (h *orderingListenerHandler) OnStartPartitionSessionRequest(
	_ context.Context,
	event *topiclistener.EventStartPartitionSession,
) error {
	h.firstSessionOnce.Do(func() {
		h.firstSession.Store(event.PartitionSession.PartitionSessionID)
	})
	h.events <- orderingListenerEvent{
		kind: "start", session: event.PartitionSession.PartitionSessionID,
		topic: event.PartitionSession.TopicPath, partition: event.PartitionSession.PartitionID,
		offset: event.CommittedOffset,
	}
	event.Confirm()

	return nil
}

func (h *orderingListenerHandler) OnReadMessages(ctx context.Context, event *topiclistener.ReadMessages) error {
	data, err := io.ReadAll(event.Batch.Messages[0])
	session := event.PartitionSession.PartitionSessionID
	h.events <- orderingListenerEvent{
		kind: "read", session: session, data: string(data), offset: event.Batch.Messages[0].Offset, err: err,
	}
	if session == h.firstSession.Load() && string(data) == "in-flight" {
		close(h.readStarted)
		select {
		case <-ctx.Done():
			h.events <- orderingListenerEvent{kind: "canceled", session: session, err: ctx.Err()}
			if h.returnCanceledError {
				return ctx.Err()
			}
		case <-h.releaseRead:
			event.Confirm()
			h.events <- orderingListenerEvent{kind: "finished", session: session, err: ctx.Err()}
		}

		return nil
	}
	if session != h.firstSession.Load() && string(data) != "independent" {
		h.events <- orderingListenerEvent{kind: "committed", session: session, err: event.ConfirmWithAck(ctx)}
	}
	if string(data) == "independent" {
		close(h.independentRead)
	}
	if session == h.firstSession.Load() && string(data) == "buffered" {
		event.Confirm()
	}

	return err
}

func (h *orderingListenerHandler) OnStopPartitionSessionRequest(
	ctx context.Context,
	event *topiclistener.EventStopPartitionSession,
) error {
	h.events <- orderingListenerEvent{kind: "stop", session: event.PartitionSession.PartitionSessionID, err: ctx.Err()}
	if event.Graceful {
		event.Confirm()
	}

	return nil
}

type orderingTopicServer struct {
	Ydb_Topic_V1.UnimplementedTopicServiceServer

	readStarted     <-chan struct{}
	independent     *orderingPartition
	independentRead <-chan struct{}
	stopGraceful    bool
	stopSent        chan struct{}
	errors          chan error
}

func (s *orderingTopicServer) StreamRead(stream Ydb_Topic_V1.TopicService_StreamReadServer) error {
	err := s.serve(stream)
	if err != nil && stream.Context().Err() == nil {
		s.errors <- err
	}

	return err
}

func (s *orderingTopicServer) serve(stream Ydb_Topic_V1.TopicService_StreamReadServer) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	if request.GetInitRequest() == nil {
		return fmt.Errorf("first request must initialize the read session, got %T", request.GetClientMessage())
	}
	if err := stream.Send(&Ydb_Topic.StreamReadMessage_FromServer{
		Status: Ydb.StatusIds_SUCCESS,
		ServerMessage: &Ydb_Topic.StreamReadMessage_FromServer_InitResponse{
			InitResponse: &Ydb_Topic.StreamReadMessage_InitResponse{SessionId: "test-read-session"},
		},
	}); err != nil {
		return err
	}
	if err := sendOrderingPartitionStart(stream, 101, "test-topic", 7, 0); err != nil {
		return err
	}
	if err := receiveOrderingPartitionStart(stream, 101); err != nil {
		return err
	}
	if err := sendOrderingPartitionRead(stream, 101, 0, "in-flight"); err != nil {
		return err
	}
	select {
	case <-s.readStarted:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	if s.independent != nil {
		if err := sendOrderingPartitionStart(stream, 303, s.independent.topic, s.independent.partition, 0); err != nil {
			return err
		}
		if err := receiveOrderingPartitionStart(stream, 303); err != nil {
			return err
		}
		if err := sendOrderingPartitionRead(stream, 303, 0, "independent"); err != nil {
			return err
		}
		select {
		case <-s.independentRead:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
	if err := sendOrderingPartitionRead(stream, 101, 1, "buffered"); err != nil {
		return err
	}
	if err := stream.Send(&Ydb_Topic.StreamReadMessage_FromServer{
		Status: Ydb.StatusIds_SUCCESS,
		ServerMessage: &Ydb_Topic.StreamReadMessage_FromServer_StopPartitionSessionRequest{
			StopPartitionSessionRequest: &Ydb_Topic.StreamReadMessage_StopPartitionSessionRequest{
				PartitionSessionId: 101,
				Graceful:           s.stopGraceful,
			},
		},
	}); err != nil {
		return err
	}
	if s.stopGraceful {
		close(s.stopSent)
		if err := receiveOrderingPartitionStop(stream, 101); err != nil {
			return err
		}
	}
	var committedOffset int64
	if s.stopGraceful {
		committedOffset = 2
	}
	if err := sendOrderingPartitionStart(stream, 202, "test-topic", 7, committedOffset); err != nil {
		return err
	}
	if err := receiveOrderingPartitionStart(stream, 202); err != nil {
		return err
	}
	for offset := committedOffset; offset < 3; offset++ {
		data := []string{"in-flight", "buffered", "new-session"}[offset]
		if err := sendOrderingPartitionRead(stream, 202, offset, data); err != nil {
			return err
		}
		if err := receiveOrderingCommit(stream, 202, offset); err != nil {
			return err
		}
	}
	<-stream.Context().Done()

	return stream.Context().Err()
}

func receiveOrderingCommit(stream Ydb_Topic_V1.TopicService_StreamReadServer, session, offset int64) error {
	for {
		request, err := stream.Recv()
		if err != nil {
			return err
		}
		commit := request.GetCommitOffsetRequest()
		if commit == nil {
			continue
		}
		if len(commit.GetCommitOffsets()) != 1 || commit.GetCommitOffsets()[0].GetPartitionSessionId() != session {
			return fmt.Errorf("unexpected commit: %v", commit)
		}
		offsets := commit.GetCommitOffsets()[0].GetOffsets()
		if len(offsets) != 1 || offsets[0].GetStart() != offset || offsets[0].GetEnd() != offset+1 {
			return fmt.Errorf("unexpected committed offsets: %v", offsets)
		}

		return sendOrderingCommitAck(stream, session, offset+1)
	}
}

func receiveOrderingPartitionStop(stream Ydb_Topic_V1.TopicService_StreamReadServer, session int64) error {
	var committed int64
	for {
		request, err := stream.Recv()
		if err != nil {
			return err
		}
		if stop := request.GetStopPartitionSessionResponse(); stop != nil {
			if stop.GetPartitionSessionId() != session || committed != 2 {
				return fmt.Errorf("stopped session %d with committed offset %d, want session %d at offset 2",
					stop.GetPartitionSessionId(), committed, session)
			}

			return nil
		}
		for _, partition := range request.GetCommitOffsetRequest().GetCommitOffsets() {
			if partition.GetPartitionSessionId() != session {
				return fmt.Errorf("unexpected commit before graceful Stop: %v", partition)
			}
			for _, offsets := range partition.GetOffsets() {
				if offsets.GetStart() > committed {
					return fmt.Errorf("commit skipped old messages: %v", offsets)
				}
				committed = max(committed, offsets.GetEnd())
			}
			if err := sendOrderingCommitAck(stream, session, committed); err != nil {
				return err
			}
		}
	}
}

func sendOrderingCommitAck(stream Ydb_Topic_V1.TopicService_StreamReadServer, session, offset int64) error {
	return stream.Send(&Ydb_Topic.StreamReadMessage_FromServer{
		Status: Ydb.StatusIds_SUCCESS,
		ServerMessage: &Ydb_Topic.StreamReadMessage_FromServer_CommitOffsetResponse{
			CommitOffsetResponse: &Ydb_Topic.StreamReadMessage_CommitOffsetResponse{
				PartitionsCommittedOffsets: []*Ydb_Topic.StreamReadMessage_CommitOffsetResponse_PartitionCommittedOffset{
					{PartitionSessionId: session, CommittedOffset: offset},
				},
			},
		},
	})
}

func sendOrderingPartitionStart(
	stream Ydb_Topic_V1.TopicService_StreamReadServer,
	session int64,
	topic string,
	partition int64,
	committedOffset int64,
) error {
	return stream.Send(&Ydb_Topic.StreamReadMessage_FromServer{
		Status: Ydb.StatusIds_SUCCESS,
		ServerMessage: &Ydb_Topic.StreamReadMessage_FromServer_StartPartitionSessionRequest{
			StartPartitionSessionRequest: &Ydb_Topic.StreamReadMessage_StartPartitionSessionRequest{
				PartitionSession: &Ydb_Topic.StreamReadMessage_PartitionSession{
					PartitionSessionId: session,
					Path:               topic,
					PartitionId:        partition,
				},
				CommittedOffset:  committedOffset,
				PartitionOffsets: &Ydb_Topic.OffsetsRange{End: 3},
			},
		},
	})
}

func receiveOrderingPartitionStart(stream Ydb_Topic_V1.TopicService_StreamReadServer, session int64) error {
	for {
		request, err := stream.Recv()
		if err != nil {
			return err
		}
		if start := request.GetStartPartitionSessionResponse(); start != nil {
			if start.GetPartitionSessionId() != session {
				return fmt.Errorf("started session = %d, want %d", start.GetPartitionSessionId(), session)
			}

			return nil
		}
	}
}

func sendOrderingPartitionRead(
	stream Ydb_Topic_V1.TopicService_StreamReadServer,
	session, offset int64,
	data string,
) error {
	return stream.Send(&Ydb_Topic.StreamReadMessage_FromServer{
		Status: Ydb.StatusIds_SUCCESS,
		ServerMessage: &Ydb_Topic.StreamReadMessage_FromServer_ReadResponse{
			ReadResponse: &Ydb_Topic.StreamReadMessage_ReadResponse{
				BytesSize: int64(len(data)),
				PartitionData: []*Ydb_Topic.StreamReadMessage_ReadResponse_PartitionData{
					{
						PartitionSessionId: session,
						Batches: []*Ydb_Topic.StreamReadMessage_ReadResponse_Batch{
							{
								Codec: int32(Ydb_Topic.Codec_CODEC_RAW),
								MessageData: []*Ydb_Topic.StreamReadMessage_ReadResponse_MessageData{
									{Offset: offset, Data: []byte(data), UncompressedSize: int64(len(data))},
								},
							},
						},
					},
				},
			},
		},
	})
}
