package topiclistenerinternal

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rekby/fixenv"
	"github.com/rekby/fixenv/sf"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestStreamListener_CreditBalanceTracksSendAndClose(t *testing.T) {
	t.Run("successful send", func(t *testing.T) {
		e := fixenv.New(t)
		ctx := sf.Context(e)
		listener := StreamListener(e)
		listener.cfg.ReaderInfo = topicreadercommon.ReaderInfo{
			Endpoint:   "node:2135",
			Database:   "/db",
			Consumer:   "consumer",
			ReaderName: "reader",
		}
		creditDeltas := make(chan int, 4)
		listener.tracer = &trace.Topic{
			OnReaderCreditBalanceChanged: func(info trace.TopicReaderCreditBalanceChangedInfo) {
				creditDeltas <- info.BytesDelta
			},
		}

		StreamMock(e).EXPECT().Send(&rawtopicreader.ReadRequest{BytesSize: 17}).Return(nil)
		listener.background.Start("metrics send loop", listener.sendMessagesLoop)
		listener.sendMessage(&rawtopicreader.ReadRequest{BytesSize: 17})

		require.Equal(t, 17, listenerMetricDelta(t, creditDeltas))
		require.NoError(t, listener.Close(ctx, errors.New("test close")))
		require.Equal(t, -17, listenerMetricDelta(t, creditDeltas))

		listener.changeCreditBalance(1)
		listenerMetricNoDelta(t, creditDeltas)
	})

	t.Run("failed send", func(t *testing.T) {
		e := fixenv.New(t)
		listener := StreamListener(e)
		creditDeltas := make(chan int, 1)
		listener.tracer = &trace.Topic{
			OnReaderCreditBalanceChanged: func(info trace.TopicReaderCreditBalanceChangedInfo) {
				creditDeltas <- info.BytesDelta
			},
		}

		StreamMock(e).EXPECT().Send(&rawtopicreader.ReadRequest{BytesSize: 17}).Return(errors.New("send failed"))
		listener.background.Start("metrics send loop", listener.sendMessagesLoop)
		listener.sendMessage(&rawtopicreader.ReadRequest{BytesSize: 17})

		select {
		case <-listener.background.Context().Done():
		case <-time.After(time.Second):
			t.Fatal("listener did not stop after failed send")
		}
		listenerMetricNoDelta(t, creditDeltas)
	})
}

func TestStreamListener_ReceivedBytesUsesProtocolSizeForDroppedBatch(t *testing.T) {
	e := fixenv.New(t)
	listener := StreamListener(e)
	ctx, cancel := context.WithCancel(sf.Context(e))
	listener.cfg.ReaderInfo = topicreadercommon.ReaderInfo{
		Endpoint:   "node:2135",
		Database:   "/db",
		Consumer:   "consumer",
		ReaderName: "reader",
	}
	listener.tracer = &trace.Topic{}

	receivedBytes := make(chan int, 1)
	creditDeltas := make(chan int, 4)
	listener.tracer.OnReaderReceivedBytes = func(info trace.TopicReaderReceivedBytesInfo) {
		receivedBytes <- info.Bytes
		cancel()
	}
	listener.tracer.OnReaderCreditBalanceChanged = func(info trace.TopicReaderCreditBalanceChangedInfo) {
		creditDeltas <- info.BytesDelta
	}

	// No worker is registered for this valid session, so the message is dropped after
	// the receive-side byte and credit traces have already been emitted.
	session := PartitionSession(e)
	session.Topic = "/topic"
	response := listenerMetricResponse(session, 50)
	StreamMock(e).EXPECT().Recv().Return(response, nil)

	done := make(chan struct{})
	go func() {
		listener.receiveMessagesLoop(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("listener receive loop did not stop")
	}

	require.Equal(t, 50, listenerMetricDelta(t, receivedBytes))
	require.Equal(t, -50, listenerMetricDelta(t, creditDeltas))
	require.NoError(t, listener.Close(context.Background(), errors.New("test close")))
	require.Equal(t, 50, listenerMetricDelta(t, creditDeltas))
	listener.changeCreditBalance(1)
	listenerMetricNoDelta(t, creditDeltas)
}

func TestStreamListener_LocalBufferTracksQueueAndHandler(t *testing.T) {
	e := fixenv.New(t)
	ctx := sf.Context(e)
	listener := StreamListener(e)
	t.Cleanup(func() {
		_ = listener.Close(context.Background(), errors.New("test cleanup"))
	})
	listener.cfg = &StreamListenerConfig{
		Decoders: topicreadercommon.NewMultiDecoder(),
		ReaderInfo: topicreadercommon.ReaderInfo{
			Endpoint:   "node:2135",
			Database:   "/db",
			Consumer:   "consumer",
			ReaderName: "reader",
		},
	}
	var (
		mu          sync.Mutex
		localDeltas []int
	)
	listener.tracer = &trace.Topic{
		OnReaderLocalBufferChanged: func(info trace.TopicReaderLocalBufferChangedInfo) {
			mu.Lock()
			localDeltas = append(localDeltas, info.MessagesDelta)
			mu.Unlock()
		},
	}

	handlerDeltas := make(chan []int, 1)
	EventHandlerMock(e).EXPECT().OnReadMessages(
		gomock.Any(), gomock.Any(),
	).DoAndReturn(func(context.Context, *PublicReadMessages) error {
		mu.Lock()
		deltas := append([]int(nil), localDeltas...)
		mu.Unlock()
		handlerDeltas <- deltas

		return nil
	})

	session := PartitionSession(e)
	session.Topic = "/topic"
	listener.createWorkerForPartition(session)
	require.NoError(t, listener.splitAndRouteReadResponse(listenerMetricResponse(session, 50)))
	select {
	case deltas := <-handlerDeltas:
		require.Equal(t, []int{1, -1}, deltas)
	case <-time.After(time.Second):
		t.Fatal("listener handler did not receive the batch")
	}

	mu.Lock()
	require.Equal(t, []int{1, -1}, localDeltas)
	mu.Unlock()
	require.NoError(t, listener.Close(ctx, errors.New("test close")))
}

func TestStreamListener_LocalBufferRollbackAfterFinalization(t *testing.T) {
	e := fixenv.New(t)
	ctx := sf.Context(e)
	listener := StreamListener(e)
	t.Cleanup(func() {
		_ = listener.Close(context.Background(), errors.New("test cleanup"))
	})
	session := PartitionSession(e)
	session.Topic = "/topic"
	listener.cfg = &StreamListenerConfig{
		Decoders: topicreadercommon.NewMultiDecoder(),
		ReaderInfo: topicreadercommon.ReaderInfo{
			Endpoint:   "node:2135",
			Database:   "/db",
			Consumer:   "consumer",
			ReaderName: "reader",
		},
	}
	var (
		mu          sync.Mutex
		localDeltas []int
	)
	listener.tracer = &trace.Topic{
		OnReaderLocalBufferChanged: func(info trace.TopicReaderLocalBufferChangedInfo) {
			mu.Lock()
			localDeltas = append(localDeltas, info.MessagesDelta)
			mu.Unlock()
			if info.MessagesDelta > 0 {
				require.NoError(t, listenerMetricCloseWorkerQueueAndFinalize(listener, session))
			}
		},
	}

	worker := NewPartitionWorker(
		session.StreamPartitionSessionID,
		session,
		listener,
		listener.handler,
		listener.onWorkerStopped,
		listener.tracer,
		listener.listenerID,
	)
	worker.readerInfo = listener.cfg.ReaderInfo
	listener.m.WithLock(func() {
		listener.workers[session.StreamPartitionSessionID] = worker
	})

	require.NoError(t, listener.splitAndRouteReadResponse(listenerMetricResponse(session, 50)))
	mu.Lock()
	require.Equal(t, []int{1, -1}, localDeltas)
	mu.Unlock()
	require.NoError(t, listener.Close(ctx, errors.New("test close")))
}

func TestStreamListener_CommitMetricsRegisterBeforeSend(t *testing.T) {
	t.Run("Confirm", func(t *testing.T) {
		e := fixenv.New(t)
		ctx := sf.Context(e)
		listener := StreamListener(e)
		t.Cleanup(func() {
			_ = listener.Close(context.Background(), errors.New("test cleanup"))
		})
		listener.cfg = &StreamListenerConfig{
			Decoders: topicreadercommon.NewMultiDecoder(),
			ReaderInfo: topicreadercommon.ReaderInfo{
				Endpoint:   "node:2135",
				Database:   "/db",
				Consumer:   "consumer",
				ReaderName: "reader",
			},
		}
		queued := make(chan int, 4)
		acknowledged := make(chan int, 4)
		listener.tracer = &trace.Topic{
			OnReaderCommitQueued: func(info trace.TopicReaderCommitQueuedInfo) {
				queued <- info.MessagesCount
			},
			OnReaderCommitAcknowledged: func(info trace.TopicReaderCommitAcknowledgedInfo) {
				acknowledged <- info.MessagesCount
			},
		}
		session := listenerMetricStartPartition(t, e, listener)
		batch := listenerMetricCommitBatch(t, listener.cfg.Decoders, session)

		StreamMock(e).EXPECT().Send(gomock.AssignableToTypeOf(&rawtopicreader.CommitOffsetRequest{})).
			DoAndReturn(func(msg rawtopicreader.ClientMessage) error {
				request := msg.(*rawtopicreader.CommitOffsetRequest)
				require.Equal(t, session.StreamPartitionSessionID, request.CommitOffsets[0].PartitionSessionID)
				require.Equal(t, rawtopiccommon.NewOffset(0), request.CommitOffsets[0].Offsets[0].Start)
				require.Equal(t, rawtopiccommon.NewOffset(5), request.CommitOffsets[0].Offsets[0].End)

				if err := listener.onCommitResponse(
					listenerMetricCommitResponse(session, rawydb.StatusInternalError, 5),
				); err != nil {
					return err
				}

				return listener.onCommitResponse(listenerMetricCommitResponse(session, rawydb.StatusSuccess, 5))
			})

		event := NewPublicReadMessages(session.ToPublic(), batch, listener)
		event.Confirm()
		// The wire range is checked above as [0, 5), so its span is five.
		require.Equal(t, 5, listenerMetricDelta(t, queued))
		require.Equal(t, 5, listenerMetricDelta(t, acknowledged))
		require.NoError(t, listener.onCommitResponse(listenerMetricCommitResponse(session, rawydb.StatusSuccess, 5)))
		require.NoError(t, listener.onCommitResponse(listenerMetricCommitResponse(session, rawydb.StatusSuccess, 4)))
		listenerMetricNoDelta(t, acknowledged)
		event.Confirm()
		listenerMetricNoDelta(t, queued)
		require.NoError(t, listener.Close(ctx, errors.New("test close")))
		topicreadercommon.TraceCommitQueued(ctx, topicreadercommon.GetCommitRange(batch))
		topicreadercommon.TraceCommitAcknowledged(ctx, session, rawtopiccommon.NewOffset(5))
		listenerMetricNoDelta(t, queued)
		listenerMetricNoDelta(t, acknowledged)
	})

	t.Run("ConfirmWithAck", func(t *testing.T) {
		e := fixenv.New(t)
		ctx, cancel := context.WithTimeout(sf.Context(e), time.Second)
		defer cancel()
		listener := StreamListener(e)
		t.Cleanup(func() {
			_ = listener.Close(context.Background(), errors.New("test cleanup"))
		})
		listener.cfg = &StreamListenerConfig{
			Decoders: topicreadercommon.NewMultiDecoder(),
			ReaderInfo: topicreadercommon.ReaderInfo{
				Endpoint:   "node:2135",
				Database:   "/db",
				Consumer:   "consumer",
				ReaderName: "reader",
			},
		}
		queued := make(chan int, 4)
		acknowledged := make(chan int, 4)
		listener.tracer = &trace.Topic{
			OnReaderCommitQueued: func(info trace.TopicReaderCommitQueuedInfo) {
				queued <- info.MessagesCount
			},
			OnReaderCommitAcknowledged: func(info trace.TopicReaderCommitAcknowledgedInfo) {
				acknowledged <- info.MessagesCount
			},
		}
		session := listenerMetricStartPartition(t, e, listener)
		batch := listenerMetricCommitBatch(t, listener.cfg.Decoders, session)

		StreamMock(e).EXPECT().Send(gomock.AssignableToTypeOf(&rawtopicreader.CommitOffsetRequest{})).
			DoAndReturn(func(msg rawtopicreader.ClientMessage) error {
				return listener.onCommitResponse(listenerMetricCommitResponse(session, rawydb.StatusSuccess, 5))
			})

		event := NewPublicReadMessages(session.ToPublic(), batch, listener)
		require.NoError(t, event.ConfirmWithAck(ctx))
		require.Equal(t, 5, listenerMetricDelta(t, queued))
		require.Equal(t, 5, listenerMetricDelta(t, acknowledged))
		require.NoError(t, listener.Close(ctx, errors.New("test close")))
		topicreadercommon.TraceCommitQueued(ctx, topicreadercommon.GetCommitRange(batch))
		topicreadercommon.TraceCommitAcknowledged(ctx, session, rawtopiccommon.NewOffset(5))
		listenerMetricNoDelta(t, queued)
		listenerMetricNoDelta(t, acknowledged)
	})
}

func TestStreamListener_MetricBalancesArePerStreamForSameReaderName(t *testing.T) {
	e := fixenv.New(t)
	listener1 := StreamListener(e)
	listener2 := newStreamMetricsListener(e, "second-listener")
	t.Cleanup(func() {
		_ = listener2.Close(context.Background(), errors.New("test cleanup"))
	})

	listener1.cfg.ReaderInfo = streamMetricsReaderInfo()
	listener2.cfg.ReaderInfo = streamMetricsReaderInfo()
	listener1.tracer = &trace.Topic{}
	listener2.tracer = &trace.Topic{}
	listener1Credits := make(chan int, 4)
	listener1Locals := make(chan int, 4)
	listener2Credits := make(chan int, 4)
	listener2Locals := make(chan int, 4)
	listener1.tracer.OnReaderCreditBalanceChanged = func(info trace.TopicReaderCreditBalanceChangedInfo) {
		listener1Credits <- info.BytesDelta
	}
	listener1.tracer.OnReaderLocalBufferChanged = func(info trace.TopicReaderLocalBufferChangedInfo) {
		listener1Locals <- info.MessagesDelta
	}
	listener2.tracer.OnReaderCreditBalanceChanged = func(info trace.TopicReaderCreditBalanceChangedInfo) {
		listener2Credits <- info.BytesDelta
	}
	listener2.tracer.OnReaderLocalBufferChanged = func(info trace.TopicReaderLocalBufferChangedInfo) {
		listener2Locals <- info.MessagesDelta
	}

	listener1.changeCreditBalance(10)
	require.True(t, listener1.reserveLocalBuffer("/topic", 2))
	listener2.changeCreditBalance(10)
	require.True(t, listener2.reserveLocalBuffer("/topic", 2))
	require.Equal(t, 10, listenerMetricDelta(t, listener1Credits))
	require.Equal(t, 2, listenerMetricDelta(t, listener1Locals))
	require.Equal(t, 10, listenerMetricDelta(t, listener2Credits))
	require.Equal(t, 2, listenerMetricDelta(t, listener2Locals))

	require.NoError(t, listener1.Close(context.Background(), errors.New("close first listener")))
	require.Equal(t, -10, listenerMetricDelta(t, listener1Credits))
	require.Equal(t, -2, listenerMetricDelta(t, listener1Locals))
	listenerMetricNoDelta(t, listener2Credits)
	listenerMetricNoDelta(t, listener2Locals)

	listener1.changeCreditBalance(1)
	require.NoError(t, listener2.Close(context.Background(), errors.New("close second listener")))
	require.Equal(t, -10, listenerMetricDelta(t, listener2Credits))
	require.Equal(t, -2, listenerMetricDelta(t, listener2Locals))
}

func listenerMetricResponse(session *topicreadercommon.PartitionSession, bytesSize int) *rawtopicreader.ReadResponse {
	return &rawtopicreader.ReadResponse{
		ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{Status: rawydb.StatusSuccess},
		BytesSize:             bytesSize,
		PartitionData: []rawtopicreader.PartitionData{
			{
				PartitionSessionID: session.StreamPartitionSessionID,
				Batches: []rawtopicreader.Batch{
					{
						Codec: rawtopiccommon.CodecRaw,
						MessageData: []rawtopicreader.MessageData{
							{
								Offset:           session.LastReceivedMessageOffset() + 1,
								Data:             []byte("message"),
								UncompressedSize: 7,
							},
						},
					},
				},
			},
		},
	}
}

func listenerMetricCommitBatch(
	t *testing.T,
	decoders *topicreadercommon.MultiDecoder,
	session *topicreadercommon.PartitionSession,
) *topicreadercommon.PublicBatch {
	t.Helper()

	batch, err := topicreadercommon.NewBatchFromStream(
		decoders,
		session,
		rawtopicreader.Batch{
			Codec: rawtopiccommon.CodecRaw,
			MessageData: []rawtopicreader.MessageData{
				{Offset: rawtopiccommon.NewOffset(0), Data: []byte("message"), UncompressedSize: 7},
				{Offset: rawtopiccommon.NewOffset(4), Data: []byte("message"), UncompressedSize: 7},
			},
		},
	)
	require.NoError(t, err)

	return batch
}

func listenerMetricStartPartition(
	t *testing.T,
	e fixenv.Env,
	listener *streamListener,
) *topicreadercommon.PartitionSession {
	t.Helper()

	const sessionID = rawtopicreader.PartitionSessionID(100)
	startSent := make(chan struct{})
	EventHandlerMock(e).EXPECT().OnStartPartitionSessionRequest(
		gomock.Any(), gomock.Any(),
	).DoAndReturn(func(_ context.Context, event *PublicEventStartPartitionSession) error {
		event.Confirm()

		return nil
	})
	StreamMock(e).EXPECT().Send(gomock.AssignableToTypeOf(&rawtopicreader.StartPartitionSessionResponse{})).
		DoAndReturn(func(rawtopicreader.ClientMessage) error {
			close(startSent)

			return nil
		})
	listener.background.Start("metrics test listener send loop", listener.sendMessagesLoop)
	require.NoError(t, listener.handleStartPartition(sf.Context(e), &rawtopicreader.StartPartitionSessionRequest{
		PartitionSession: rawtopicreader.PartitionSession{
			PartitionSessionID: sessionID,
			Path:               "/topic",
			PartitionID:        1,
		},
		CommittedOffset: rawtopiccommon.NewOffset(0),
	}))
	select {
	case <-startSent:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for listener partition start response")
	}

	session, err := listener.sessions.Get(sessionID)
	require.NoError(t, err)

	return session
}

func listenerMetricCommitResponse(
	session *topicreadercommon.PartitionSession,
	status rawydb.StatusCode,
	offset int64,
) *rawtopicreader.CommitOffsetResponse {
	return &rawtopicreader.CommitOffsetResponse{
		ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{Status: status},
		PartitionsCommittedOffsets: []rawtopicreader.PartitionCommittedOffset{{
			PartitionSessionID: session.StreamPartitionSessionID,
			CommittedOffset:    rawtopiccommon.NewOffset(offset),
		}},
	}
}

func listenerMetricDelta(t *testing.T, deltas <-chan int) int {
	t.Helper()

	select {
	case delta := <-deltas:
		return delta
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for listener metric delta")

		return 0
	}
}

func listenerMetricNoDelta(t *testing.T, deltas <-chan int) {
	t.Helper()

	select {
	case delta := <-deltas:
		t.Fatalf("unexpected listener metric delta %d", delta)
	default:
	}
}

func listenerMetricCloseWorkerQueueAndFinalize(
	listener *streamListener,
	session *topicreadercommon.PartitionSession,
) error {
	var worker *PartitionWorker
	listener.m.WithLock(func() {
		worker = listener.workers[session.StreamPartitionSessionID]
	})
	if worker == nil {
		return errors.New("metrics test worker is missing")
	}
	worker.messageQueue.Close()
	listener.finalizeLocalBuffer()

	return nil
}

func streamMetricsReaderInfo() topicreadercommon.ReaderInfo {
	return topicreadercommon.ReaderInfo{
		Endpoint:   "node:2135",
		Database:   "/db",
		Consumer:   "consumer",
		ReaderName: "same-reader",
	}
}

func newStreamMetricsListener(e fixenv.Env, listenerID string) *streamListener {
	listener := &streamListener{
		cfg: &StreamListenerConfig{
			Decoders: topicreadercommon.NewMultiDecoder(),
		},
		stream:      StreamMock(e),
		streamClose: func(error) {},
		handler:     EventHandlerMock(e),
		tracer:      &trace.Topic{},
		listenerID:  listenerID,
		sessionID:   "test-session-id",
	}
	listener.initVars(&atomic.Int64{})
	listener.syncCommitter = topicreadercommon.NewCommitterStopped(
		listener.tracer,
		sf.Context(e),
		topicreadercommon.CommitModeSync,
		listener.stream.Send,
	)
	listener.syncCommitter.Start()
	listener.background = *background.NewWorker(sf.Context(e), "metrics-test-listener")

	return listener
}
