package topicreaderinternal

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTopicStreamReader_CreditBalanceTracksSendReceiveAndClose(t *testing.T) {
	e := newTopicReaderTestEnv(t)
	e.reader.cfg.ReaderInfo = topicreadercommon.ReaderInfo{
		Endpoint:   "node:2135",
		Database:   "/db",
		Consumer:   "consumer",
		ReaderName: "reader",
	}

	creditDeltas := make(chan int, 8)
	e.reader.cfg.Trace = &trace.Topic{
		OnReaderCreditBalanceChanged: func(info trace.TopicReaderCreditBalanceChangedInfo) {
			creditDeltas <- info.BytesDelta
		},
	}

	// The stopped-reader fixture starts with its initial credit queued. Replace it
	// with a smaller request so the test can account for every balance transition.
	<-e.reader.freeBytes
	const requestSize = 100
	e.stream.EXPECT().Send(&rawtopicreader.ReadRequest{BytesSize: requestSize}).Return(nil)

	loopCtx, cancelLoop := context.WithCancel(e.ctx)
	loopDone := make(chan struct{})
	go func() {
		e.reader.dataRequestLoop(loopCtx)
		close(loopDone)
	}()
	e.reader.freeBytes <- requestSize

	require.Equal(t, requestSize, readerMetricDelta(t, creditDeltas))
	require.NoError(t, e.reader.onReadResponse(readerMetricResponse(&e, 40)))
	require.Equal(t, -40, readerMetricDelta(t, creditDeltas))

	require.NoError(t, e.reader.CloseWithError(e.ctx, errors.New("test close")))
	require.Equal(t, -60, readerMetricDelta(t, creditDeltas))
	e.reader.finalizeCreditBalance()

	// A late callback cannot revive a finalized balance or emit another close delta.
	e.reader.changeCreditBalance(5)
	readerMetricNoDelta(t, creditDeltas)

	cancelLoop()
	select {
	case <-loopDone:
	case <-time.After(time.Second):
		t.Fatal("reader data request loop did not stop")
	}
}

func TestTopicStreamReader_CreditBalanceIgnoresFailedSend(t *testing.T) {
	e := newTopicReaderTestEnv(t)
	creditDeltas := make(chan int, 1)
	e.reader.cfg.Trace = &trace.Topic{
		OnReaderCreditBalanceChanged: func(info trace.TopicReaderCreditBalanceChangedInfo) {
			creditDeltas <- info.BytesDelta
		},
	}

	<-e.reader.freeBytes
	const requestSize = 17
	e.stream.EXPECT().Send(&rawtopicreader.ReadRequest{BytesSize: requestSize}).Return(errors.New("send failed"))

	loopDone := make(chan struct{})
	go func() {
		e.reader.dataRequestLoop(e.ctx)
		close(loopDone)
	}()
	e.reader.freeBytes <- requestSize

	select {
	case <-loopDone:
	case <-time.After(time.Second):
		t.Fatal("reader data request loop did not stop after failed send")
	}
	readerMetricNoDelta(t, creditDeltas)
}

func TestTopicStreamReader_ReceivedBytesUsesProtocolSizeWhenBatchIsDropped(t *testing.T) {
	e := newTopicReaderTestEnv(t)
	receivedBytes := make(chan int, 1)
	e.reader.cfg.Trace = &trace.Topic{
		OnReaderReceivedBytes: func(info trace.TopicReaderReceivedBytesInfo) {
			receivedBytes <- info.Bytes
		},
	}

	response := readerMetricResponse(&e, 50)
	response.PartitionData[0].PartitionSessionID++
	require.Error(t, e.reader.onReadResponse(response))
	require.Equal(t, 50, readerMetricDelta(t, receivedBytes))
}

func TestTopicStreamReader_LocalBufferTracksQueueAndDelivery(t *testing.T) {
	e := newTopicReaderTestEnv(t)
	e.reader.cfg.ReaderInfo.ReaderName = "reader"

	var (
		mu                    sync.Mutex
		localDeltas           []int
		queueEmptyAtReserve   bool
		deliveredOrderCorrect bool
	)
	e.reader.cfg.Trace = &trace.Topic{
		OnReaderLocalBufferChanged: func(info trace.TopicReaderLocalBufferChangedInfo) {
			queueEmpty := false
			if info.MessagesDelta > 0 {
				e.reader.batcher.m.WithLock(func() {
					queueEmpty = len(e.reader.batcher.messages) == 0
				})
			}
			mu.Lock()
			if info.MessagesDelta > 0 {
				queueEmptyAtReserve = queueEmpty
			}
			localDeltas = append(localDeltas, info.MessagesDelta)
			mu.Unlock()
		},
		OnReaderMessagesDelivered: func(trace.TopicReaderMessagesDeliveredInfo) {
			mu.Lock()
			deliveredOrderCorrect = len(localDeltas) == 2 && localDeltas[0] == 1 && localDeltas[1] == -1
			mu.Unlock()
		},
	}

	e.stream.EXPECT().Send(&rawtopicreader.ReadRequest{BytesSize: 50}).Return(nil)
	e.Start()
	reader := newMetricsReader(&e)
	readResult := make(chan readerBatchResult, 1)
	go func() {
		batch, err := reader.ReadMessageBatch(e.ctx)
		readResult <- readerBatchResult{batch: batch, err: err}
	}()
	e.SendFromServer(readerMetricResponse(&e, 50))
	var result readerBatchResult
	select {
	case result = <-readResult:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for reader batch")
	}
	require.NoError(t, result.err)
	require.NotNil(t, result.batch)

	mu.Lock()
	observedDeltas := append([]int(nil), localDeltas...)
	observedQueueEmpty := queueEmptyAtReserve
	observedDeliveredOrder := deliveredOrderCorrect
	mu.Unlock()
	require.Equal(t, []int{1, -1}, observedDeltas)
	require.True(t, observedQueueEmpty)
	require.True(t, observedDeliveredOrder)
}

func TestTopicStreamReader_LocalBufferRollbackAfterFinalization(t *testing.T) {
	e := newTopicReaderTestEnv(t)
	var localDeltas []int
	var closeErr error
	e.reader.cfg.Trace = &trace.Topic{
		OnReaderLocalBufferChanged: func(info trace.TopicReaderLocalBufferChangedInfo) {
			localDeltas = append(localDeltas, info.MessagesDelta)
			if info.MessagesDelta > 0 {
				closeErr = e.reader.batcher.Close(errors.New("closed during reserve"))
				e.reader.finalizeLocalBuffer()
			}
		},
	}

	require.Error(t, e.reader.onReadResponse(readerMetricResponse(&e, 50)))
	require.Equal(t, []int{1, -1}, localDeltas)
	require.NoError(t, closeErr)
}

func TestTopicReader_CommitMetricsRegisterBeforeSynchronousAckAndClose(t *testing.T) {
	test := newReaderCommitMetricsTest(t)
	committedOffsets := make(chan int64, 1)
	test.env.reader.cfg.Trace.OnReaderCommittedNotify = func(trace.TopicReaderCommittedNotifyInfo) {
		committedOffsets <- test.env.partitionSession.CommittedOffset().ToInt64()
		test.env.partitionSession.Close()
	}
	commitSent := make(chan *rawtopicreader.CommitOffsetRequest, 1)
	test.env.stream.EXPECT().Send(gomock.AssignableToTypeOf(&rawtopicreader.CommitOffsetRequest{})).
		DoAndReturn(func(msg rawtopicreader.ClientMessage) error {
			request := msg.(*rawtopicreader.CommitOffsetRequest)
			commitSent <- request

			// Register the acknowledgement before Send returns. This is the
			// ordering that matters when the server replies synchronously.
			return test.env.reader.onCommitResponse(readerMetricCommitResponse(test.env, 25))
		})
	require.NoError(t, test.reader.Commit(test.env.ctx, test.batch))
	// The wire range is [20, 25), so its span is five even though this
	// fixture contains a gap between messages.
	request := readerMetricCommitRequest(t, commitSent)
	require.Equal(t, test.env.partitionSessionID, request.CommitOffsets[0].PartitionSessionID)
	require.Equal(t, rawtopiccommon.NewOffset(20), request.CommitOffsets[0].Offsets[0].Start)
	require.Equal(t, rawtopiccommon.NewOffset(25), request.CommitOffsets[0].Offsets[0].End)
	require.Equal(t, 5, readerMetricDelta(t, test.queued))
	require.Equal(t, 5, readerMetricDelta(t, test.acknowledged))
	select {
	case committedOffset := <-committedOffsets:
		require.Equal(t, int64(25), committedOffset)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for committed offset publication")
	}

	// Registration happened before the callback closed the session, so the
	// acknowledgement remains observable even after the session is closed.
	require.NoError(t, test.reader.Close(context.Background()))
	topicreadercommon.TraceCommitQueued(test.env.ctx, topicreadercommon.GetCommitRange(test.batch))
	topicreadercommon.TraceCommitAcknowledged(test.env.ctx, test.env.partitionSession, rawtopiccommon.NewOffset(25))
	readerMetricNoDelta(t, test.queued)
	readerMetricNoDelta(t, test.acknowledged)
}

func TestTopicReader_CommitMetricsIgnoreDuplicateAndStaleAcknowledgements(t *testing.T) {
	test := newReaderCommitMetricsTest(t)
	test.env.stream.EXPECT().Send(gomock.AssignableToTypeOf(&rawtopicreader.CommitOffsetRequest{})).
		Return(nil)
	require.NoError(t, test.reader.Commit(test.env.ctx, test.batch))
	require.Equal(t, 5, readerMetricDelta(t, test.queued))
	require.NoError(t, test.env.reader.onCommitResponse(&rawtopicreader.CommitOffsetResponse{
		ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{Status: rawydb.StatusInternalError},
		PartitionsCommittedOffsets: []rawtopicreader.PartitionCommittedOffset{{
			PartitionSessionID: test.env.partitionSessionID,
			CommittedOffset:    rawtopiccommon.NewOffset(25),
		}},
	}))
	readerMetricNoDelta(t, test.acknowledged)
	require.NoError(t, test.env.reader.onCommitResponse(readerMetricCommitResponse(test.env, 25)))
	require.Equal(t, 5, readerMetricDelta(t, test.acknowledged))

	// Direct calls provide a processing barrier for duplicate and stale ACKs.
	require.NoError(t, test.env.reader.onCommitResponse(readerMetricCommitResponse(test.env, 25)))
	require.NoError(t, test.env.reader.onCommitResponse(readerMetricCommitResponse(test.env, 24)))
	readerMetricNoDelta(t, test.acknowledged)
}

func TestTopicStreamReader_MetricBalancesArePerStreamForSameReaderName(t *testing.T) {
	e1 := newTopicReaderTestEnv(t)
	e2 := newTopicReaderTestEnv(t)

	type streamEvents struct {
		credits chan int
		locals  chan int
	}
	newEvents := func(reader *topicStreamReaderImpl) streamEvents {
		events := streamEvents{credits: make(chan int, 4), locals: make(chan int, 4)}
		reader.cfg.ReaderInfo = topicreadercommon.ReaderInfo{
			Endpoint:   "node:2135",
			Database:   "/db",
			Consumer:   "consumer",
			ReaderName: "same-reader",
		}
		reader.cfg.Trace = &trace.Topic{
			OnReaderCreditBalanceChanged: func(info trace.TopicReaderCreditBalanceChangedInfo) {
				events.credits <- info.BytesDelta
			},
			OnReaderLocalBufferChanged: func(info trace.TopicReaderLocalBufferChangedInfo) {
				events.locals <- info.MessagesDelta
			},
		}

		return events
	}
	events1 := newEvents(e1.reader)
	events2 := newEvents(e2.reader)

	e1.reader.changeCreditBalance(10)
	require.True(t, e1.reader.reserveLocalBuffer("/topic", 2))
	e2.reader.changeCreditBalance(10)
	require.True(t, e2.reader.reserveLocalBuffer("/topic", 2))
	require.Equal(t, 10, readerMetricDelta(t, events1.credits))
	require.Equal(t, 2, readerMetricDelta(t, events1.locals))
	require.Equal(t, 10, readerMetricDelta(t, events2.credits))
	require.Equal(t, 2, readerMetricDelta(t, events2.locals))

	require.NoError(t, e1.reader.CloseWithError(e1.ctx, errors.New("close first reader")))
	require.Equal(t, -10, readerMetricDelta(t, events1.credits))
	require.Equal(t, -2, readerMetricDelta(t, events1.locals))
	readerMetricNoDelta(t, events2.credits)
	readerMetricNoDelta(t, events2.locals)

	e1.reader.changeCreditBalance(1)
	require.NoError(t, e2.reader.CloseWithError(e2.ctx, errors.New("close second reader")))
	require.Equal(t, -10, readerMetricDelta(t, events2.credits))
	require.Equal(t, -2, readerMetricDelta(t, events2.locals))
}

func readerMetricResponse(e *streamEnv, bytesSize int) *rawtopicreader.ReadResponse {
	return readerMetricResponseWithOffsets(
		e,
		bytesSize,
		e.partitionSession.LastReceivedMessageOffset().ToInt64()+1,
	)
}

func readerMetricResponseWithOffsets(
	e *streamEnv,
	bytesSize int,
	offsets ...int64,
) *rawtopicreader.ReadResponse {
	messages := make([]rawtopicreader.MessageData, len(offsets))
	for i, offset := range offsets {
		messages[i] = rawtopicreader.MessageData{
			Offset:           rawtopiccommon.NewOffset(offset),
			Data:             []byte("message"),
			UncompressedSize: 7,
		}
	}

	return &rawtopicreader.ReadResponse{
		BytesSize: bytesSize,
		PartitionData: []rawtopicreader.PartitionData{
			{
				PartitionSessionID: e.partitionSessionID,
				Batches: []rawtopicreader.Batch{
					{
						Codec:       rawtopiccommon.CodecRaw,
						MessageData: messages,
					},
				},
			},
		},
	}
}

func readerMetricCommitResponse(e *streamEnv, offset int64) *rawtopicreader.CommitOffsetResponse {
	return &rawtopicreader.CommitOffsetResponse{
		ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{Status: rawydb.StatusSuccess},
		PartitionsCommittedOffsets: []rawtopicreader.PartitionCommittedOffset{{
			PartitionSessionID: e.partitionSessionID,
			CommittedOffset:    rawtopiccommon.NewOffset(offset),
		}},
	}
}

type readerBatchResult struct {
	batch *topicreadercommon.PublicBatch
	err   error
}

type readerCommitMetricsTest struct {
	env          *streamEnv
	reader       *Reader
	batch        *topicreadercommon.PublicBatch
	queued       chan int
	acknowledged chan int
}

func newReaderCommitMetricsTest(t *testing.T) *readerCommitMetricsTest {
	t.Helper()

	e := newTopicReaderTestEnv(t)
	e.reader.cfg.ReaderInfo = topicreadercommon.ReaderInfo{
		Endpoint:   "node:2135",
		Database:   "/db",
		Consumer:   "consumer",
		ReaderName: "reader",
	}
	queued := make(chan int, 4)
	acknowledged := make(chan int, 4)
	e.reader.cfg.Trace = &trace.Topic{
		OnReaderCommitQueued: func(info trace.TopicReaderCommitQueuedInfo) {
			queued <- info.MessagesCount
		},
		OnReaderCommitAcknowledged: func(info trace.TopicReaderCommitAcknowledgedInfo) {
			acknowledged <- info.MessagesCount
		},
	}

	startedSessionID := rawtopicreader.PartitionSessionID(42)
	require.NoError(t, e.reader.onStartPartitionSessionRequest(&rawtopicreader.StartPartitionSessionRequest{
		PartitionSession: rawtopicreader.PartitionSession{
			PartitionSessionID: startedSessionID,
			Path:               e.partitionSession.Topic,
			PartitionID:        e.partitionSession.PartitionID,
		},
		CommittedOffset: rawtopiccommon.NewOffset(20),
	}))
	startedSession, err := e.reader.sessionController.Get(startedSessionID)
	require.NoError(t, err)
	e.partitionSessionID = startedSessionID
	e.partitionSession = startedSession
	startResponseSent := make(empty.Chan)
	e.stream.EXPECT().Send(gomock.AssignableToTypeOf(&rawtopicreader.StartPartitionSessionResponse{})).
		DoAndReturn(func(_ rawtopicreader.ClientMessage) error {
			close(startResponseSent)

			return nil
		})
	e.stream.EXPECT().Send(&rawtopicreader.ReadRequest{BytesSize: 50}).Return(nil)
	e.Start()
	reader := newMetricsReader(&e)
	readResult := make(chan readerBatchResult, 1)
	go func() {
		batch, err := reader.ReadMessageBatch(e.ctx)
		readResult <- readerBatchResult{batch: batch, err: err}
	}()
	select {
	case <-startResponseSent:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for start partition session response")
	}
	e.SendFromServer(readerMetricResponseWithOffsets(&e, 50, 20, 24))
	select {
	case result := <-readResult:
		require.NoError(t, result.err)
		require.NotNil(t, result.batch)
		require.Equal(t, []int64{20, 24}, []int64{
			result.batch.Messages[0].Offset,
			result.batch.Messages[1].Offset,
		})

		return &readerCommitMetricsTest{
			env:          &e,
			reader:       reader,
			batch:        result.batch,
			queued:       queued,
			acknowledged: acknowledged,
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for reader batch")

		return nil
	}
}

func readerMetricCommitRequest(
	t *testing.T,
	requests <-chan *rawtopicreader.CommitOffsetRequest,
) *rawtopicreader.CommitOffsetRequest {
	t.Helper()

	select {
	case request := <-requests:
		return request
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for reader commit request")

		return nil
	}
}

func readerMetricDelta(t *testing.T, deltas <-chan int) int {
	t.Helper()

	select {
	case delta := <-deltas:
		return delta
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for metric delta")

		return 0
	}
}

func readerMetricNoDelta(t *testing.T, deltas <-chan int) {
	t.Helper()

	select {
	case delta := <-deltas:
		t.Fatalf("unexpected metric delta %d", delta)
	default:
	}
}

func newMetricsReader(e *streamEnv) *Reader {
	reconnector := &readerReconnector{
		streamVal:           e.reader,
		streamContextCancel: func(error) {},
		tracer:              e.reader.cfg.Trace,
		readerInfo:          e.reader.cfg.ReaderInfo,
	}
	reconnector.initChannelsAndClock()

	reader := &Reader{
		reader:             reconnector,
		defaultBatchConfig: newReadMessageBatchOptions(),
		tracer:             e.reader.cfg.Trace,
		readerID:           e.reader.readerID,
		readerInfo:         e.reader.cfg.ReaderInfo,
	}
	e.t.Cleanup(func() {
		_ = reader.Close(context.Background())
	})

	return reader
}
