package topicreaderinternal

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

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
		mu          sync.Mutex
		localDeltas []int
	)
	e.reader.cfg.Trace = &trace.Topic{
		OnReaderLocalBufferChanged: func(info trace.TopicReaderLocalBufferChangedInfo) {
			mu.Lock()
			defer mu.Unlock()
			localDeltas = append(localDeltas, info.MessagesDelta)
			if info.MessagesDelta > 0 {
				e.reader.batcher.m.WithLock(func() {
					require.Empty(t, e.reader.batcher.messages)
				})
			}
		},
		OnReaderMessagesDelivered: func(trace.TopicReaderMessagesDeliveredInfo) {
			mu.Lock()
			defer mu.Unlock()
			require.Equal(t, []int{1, -1}, localDeltas)
		},
	}

	e.stream.EXPECT().Send(&rawtopicreader.ReadRequest{BytesSize: 50}).Return(nil)
	e.Start()
	reader := newMetricsReader(&e)
	readResult := make(chan struct {
		batch *topicreadercommon.PublicBatch
		err   error
	}, 1)
	go func() {
		batch, err := reader.ReadMessageBatch(e.ctx)
		readResult <- struct {
			batch *topicreadercommon.PublicBatch
			err   error
		}{batch: batch, err: err}
	}()
	e.SendFromServer(readerMetricResponse(&e, 50))
	var result struct {
		batch *topicreadercommon.PublicBatch
		err   error
	}
	select {
	case result = <-readResult:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for reader batch")
	}
	require.NoError(t, result.err)
	require.NotNil(t, result.batch)

	mu.Lock()
	require.Equal(t, []int{1, -1}, localDeltas)
	mu.Unlock()
}

func TestTopicStreamReader_LocalBufferRollbackAfterFinalization(t *testing.T) {
	e := newTopicReaderTestEnv(t)
	var (
		mu          sync.Mutex
		localDeltas []int
	)
	e.reader.cfg.Trace = &trace.Topic{
		OnReaderLocalBufferChanged: func(info trace.TopicReaderLocalBufferChangedInfo) {
			mu.Lock()
			localDeltas = append(localDeltas, info.MessagesDelta)
			mu.Unlock()
			if info.MessagesDelta > 0 {
				require.NoError(t, e.reader.batcher.Close(errors.New("closed during reserve")))
				e.reader.finalizeLocalBuffer()
			}
		},
	}

	require.Error(t, e.reader.onReadResponse(readerMetricResponse(&e, 50)))
	mu.Lock()
	require.Equal(t, []int{1, -1}, localDeltas)
	mu.Unlock()
}

func TestTopicReader_CommitMetricsUseLogicalMessageCount(t *testing.T) {
	for _, test := range []struct {
		name            string
		closeOnPublish  bool
		checkDuplicates bool
	}{
		{
			name:            "live tracker suppresses duplicate and stale acknowledgements",
			checkDuplicates: true,
		},
		{
			name:           "acknowledged count survives close after publication",
			closeOnPublish: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			e := newTopicReaderTestEnv(t)
			e.reader.cfg.ReaderInfo = topicreadercommon.ReaderInfo{
				Endpoint:   "node:2135",
				Database:   "/db",
				Consumer:   "consumer",
				ReaderName: "reader",
			}
			queued := make(chan int, 4)
			acknowledged := make(chan int, 4)
			committedOffsets := make(chan int64, 4)
			e.reader.cfg.Trace = &trace.Topic{
				OnReaderCommitQueued: func(info trace.TopicReaderCommitQueuedInfo) {
					queued <- info.MessagesCount
				},
				OnReaderCommitAcknowledged: func(info trace.TopicReaderCommitAcknowledgedInfo) {
					acknowledged <- info.MessagesCount
				},
				OnReaderCommittedNotify: func(trace.TopicReaderCommittedNotifyInfo) {
					committedOffsets <- e.partitionSession.CommittedOffset().ToInt64()
					if test.closeOnPublish {
						e.partitionSession.Close()
					}
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
			e.stream.EXPECT().Send(gomock.AssignableToTypeOf(&rawtopicreader.StartPartitionSessionResponse{})).Return(nil)
			e.stream.EXPECT().Send(&rawtopicreader.ReadRequest{BytesSize: 50}).Return(nil)
			e.Start()
			reader := newMetricsReader(&e)

			readResult := make(chan struct {
				batch *topicreadercommon.PublicBatch
				err   error
			}, 1)
			go func() {
				batch, err := reader.ReadMessageBatch(e.ctx)
				readResult <- struct {
					batch *topicreadercommon.PublicBatch
					err   error
				}{batch: batch, err: err}
			}()
			e.SendFromServer(readerMetricResponseWithOffsets(&e, 50, 20, 24))
			var result struct {
				batch *topicreadercommon.PublicBatch
				err   error
			}
			select {
			case result = <-readResult:
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for reader batch")
			}
			require.NoError(t, result.err)
			require.NotNil(t, result.batch)
			require.Equal(t, []int64{20, 24}, []int64{
				result.batch.Messages[0].Offset,
				result.batch.Messages[1].Offset,
			})

			commitSent := make(chan *rawtopicreader.CommitOffsetRequest, 1)
			e.stream.EXPECT().Send(gomock.AssignableToTypeOf(&rawtopicreader.CommitOffsetRequest{})).
				DoAndReturn(func(msg rawtopicreader.ClientMessage) error {
					commitSent <- msg.(*rawtopicreader.CommitOffsetRequest)

					return nil
				})
			require.NoError(t, reader.Commit(e.ctx, result.batch))
			require.Equal(t, 2, readerMetricDelta(t, queued))
			select {
			case request := <-commitSent:
				require.Equal(t, e.partitionSessionID, request.CommitOffsets[0].PartitionSessionID)
				require.Equal(t, rawtopiccommon.NewOffset(20), request.CommitOffsets[0].Offsets[0].Start)
				require.Equal(t, rawtopiccommon.NewOffset(25), request.CommitOffsets[0].Offsets[0].End)
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for reader commit request")
			}

			require.NoError(t, e.reader.onCommitResponse(&rawtopicreader.CommitOffsetResponse{
				ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{Status: rawydb.StatusInternalError},
				PartitionsCommittedOffsets: []rawtopicreader.PartitionCommittedOffset{{
					PartitionSessionID: e.partitionSessionID,
					CommittedOffset:    rawtopiccommon.NewOffset(25),
				}},
			}))
			readerMetricNoDelta(t, acknowledged)
			require.NoError(t, e.reader.onCommitResponse(readerMetricCommitResponse(&e, 25)))
			require.Equal(t, 2, readerMetricDelta(t, acknowledged))
			select {
			case committedOffset := <-committedOffsets:
				require.Equal(t, int64(25), committedOffset)
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for committed offset publication")
			}

			if test.checkDuplicates {
				// Direct calls provide a processing barrier for duplicate/stale ACKs.
				require.NoError(t, e.reader.onCommitResponse(readerMetricCommitResponse(&e, 25)))
				require.NoError(t, e.reader.onCommitResponse(readerMetricCommitResponse(&e, 24)))
				readerMetricNoDelta(t, acknowledged)
			}

			require.NoError(t, reader.Close(context.Background()))
			topicreadercommon.TraceCommitQueued(e.ctx, topicreadercommon.GetCommitRange(result.batch))
			topicreadercommon.TraceCommitAcknowledged(e.ctx, e.partitionSession, rawtopiccommon.NewOffset(25))
			readerMetricNoDelta(t, queued)
			readerMetricNoDelta(t, acknowledged)
		})
	}
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
