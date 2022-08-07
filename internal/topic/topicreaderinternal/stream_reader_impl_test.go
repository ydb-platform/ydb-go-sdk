package topicreaderinternal

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"runtime/pprof"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTopicStreamReaderImpl_CommitStolen(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		e := newTopicReaderTestEnv(t)
		e.Start()

		lastOffset := e.partitionSession.lastReceivedMessageOffset()
		const dataSize = 4

		// request new data portion
		e.stream.EXPECT().Send(&rawtopicreader.ReadRequest{BytesSize: dataSize * 2})

		commitReceived := make(empty.Chan)
		// Expect commit message with stole
		e.stream.EXPECT().Send(
			&rawtopicreader.CommitOffsetRequest{
				CommitOffsets: []rawtopicreader.PartitionCommitOffset{
					{
						PartitionSessionID: e.partitionSessionID,
						Offsets: []rawtopicreader.OffsetRange{
							{
								Start: lastOffset + 1,
								End:   lastOffset + 16,
							},
						},
					},
				},
			},
		).Do(func(req *rawtopicreader.CommitOffsetRequest) {
			close(commitReceived)
		})

		// send message with stole offsets
		//
		e.SendFromServer(&rawtopicreader.ReadResponse{
			BytesSize: dataSize,
			PartitionData: []rawtopicreader.PartitionData{
				{
					PartitionSessionID: e.partitionSessionID,
					Batches: []rawtopicreader.Batch{
						{
							Codec:      rawtopiccommon.CodecRaw,
							ProducerID: "1",
							MessageData: []rawtopicreader.MessageData{
								{
									Offset: lastOffset + 10,
								},
							},
						},
					},
				},
			},
		})

		e.SendFromServer(&rawtopicreader.ReadResponse{
			BytesSize: dataSize,
			PartitionData: []rawtopicreader.PartitionData{
				{
					PartitionSessionID: e.partitionSessionID,
					Batches: []rawtopicreader.Batch{
						{
							Codec:      rawtopiccommon.CodecRaw,
							ProducerID: "1",
							MessageData: []rawtopicreader.MessageData{
								{
									Offset: lastOffset + 15,
								},
							},
						},
					},
				},
			},
		})

		opts := newReadMessageBatchOptions()
		opts.MinCount = 2
		batch, err := e.reader.ReadMessageBatch(e.ctx, opts)
		require.NoError(t, err)
		require.NoError(t, e.reader.Commit(e.ctx, batch.getCommitRange().priv))
		<-commitReceived
	})
}

func TestTopicStreamReaderImpl_Create(t *testing.T) {
	xtest.TestManyTimesWithName(t, "BadSessionInitialization", func(t testing.TB) {
		mc := gomock.NewController(t)
		stream := NewMockRawTopicReaderStream(mc)
		stream.EXPECT().Send(gomock.Any()).Return(nil)
		stream.EXPECT().Recv().Return(&rawtopicreader.StartPartitionSessionRequest{
			ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{Status: rawydb.StatusInternalError},
		}, nil)
		stream.EXPECT().CloseSend().Return(nil)

		reader, err := newTopicStreamReader(stream, newTopicStreamReaderConfig())
		require.Error(t, err)
		require.Nil(t, reader)
	})
}

func TestStreamReaderImpl_OnPartitionCloseHandle(t *testing.T) {
	xtest.TestManyTimesWithName(t, "GracefulFalseCancelPartitionContext", func(t testing.TB) {
		e := newTopicReaderTestEnv(t)
		e.Start()

		require.NoError(t, e.partitionSession.Context().Err())

		// stop partition
		e.SendFromServerAndSetNextCallback(
			&rawtopicreader.StopPartitionSessionRequest{PartitionSessionID: e.partitionSessionID},
			func() {
				require.Error(t, e.partitionSession.Context().Err())
			})
		e.WaitMessageReceived()
	})
	xtest.TestManyTimesWithName(t, "TraceGracefulTrue", func(t testing.TB) {
		e := newTopicReaderTestEnv(t)

		readMessagesCtx, readMessagesCtxCancel := xcontext.WithErrCancel(context.Background())
		committedOffset := int64(222)

		e.reader.cfg.Tracer.OnReaderPartitionReadStopResponse = func(info trace.TopicReaderPartitionReadStopResponseStartInfo) func(doneInfo trace.TopicReaderPartitionReadStopResponseDoneInfo) { //nolint:lll
			expected := trace.TopicReaderPartitionReadStopResponseStartInfo{
				ReaderConnectionID: e.reader.readConnectionID,
				PartitionContext:   e.partitionSession.ctx,
				Topic:              e.partitionSession.Topic,
				PartitionID:        e.partitionSession.PartitionID,
				PartitionSessionID: e.partitionSession.partitionSessionID.ToInt64(),
				CommittedOffset:    committedOffset,
				Graceful:           true,
			}
			require.Equal(t, expected, info)

			require.NoError(t, info.PartitionContext.Err())

			readMessagesCtxCancel(errors.New("test tracer finished"))
			return nil
		}

		e.Start()

		e.stream.EXPECT().Send(&rawtopicreader.StopPartitionSessionResponse{
			PartitionSessionID: e.partitionSessionID,
		}).Return(nil)

		e.SendFromServer(&rawtopicreader.StopPartitionSessionRequest{
			PartitionSessionID: e.partitionSessionID,
			Graceful:           true,
			CommittedOffset:    rawtopicreader.NewOffset(committedOffset),
		})

		_, err := e.reader.ReadMessageBatch(readMessagesCtx, newReadMessageBatchOptions())
		require.Error(t, err)
		require.Error(t, readMessagesCtx.Err())
	})
	xtest.TestManyTimesWithName(t, "TraceGracefulFalse", func(t testing.TB) {
		e := newTopicReaderTestEnv(t)

		readMessagesCtx, readMessagesCtxCancel := xcontext.WithErrCancel(context.Background())
		committedOffset := int64(222)

		e.reader.cfg.Tracer.OnReaderPartitionReadStopResponse = func(info trace.TopicReaderPartitionReadStopResponseStartInfo) func(doneInfo trace.TopicReaderPartitionReadStopResponseDoneInfo) { //nolint:lll
			expected := trace.TopicReaderPartitionReadStopResponseStartInfo{
				ReaderConnectionID: e.reader.readConnectionID,
				PartitionContext:   e.partitionSession.ctx,
				Topic:              e.partitionSession.Topic,
				PartitionID:        e.partitionSession.PartitionID,
				PartitionSessionID: e.partitionSession.partitionSessionID.ToInt64(),
				CommittedOffset:    committedOffset,
				Graceful:           false,
			}
			require.Equal(t, expected, info)
			require.Error(t, info.PartitionContext.Err())

			readMessagesCtxCancel(errors.New("test tracer finished"))
			return nil
		}

		e.Start()

		e.SendFromServer(&rawtopicreader.StopPartitionSessionRequest{
			PartitionSessionID: e.partitionSessionID,
			Graceful:           false,
			CommittedOffset:    rawtopicreader.NewOffset(committedOffset),
		})

		_, err := e.reader.ReadMessageBatch(readMessagesCtx, newReadMessageBatchOptions())
		require.Error(t, err)
		require.Error(t, readMessagesCtx.Err())
	})
}

func TestTopicStreamReaderImpl_ReadMessages(t *testing.T) {
	t.Run("BufferSize", func(t *testing.T) {
		waitChangeRestBufferSizeBytes := func(r *topicStreamReaderImpl, old int64) {
			xtest.SpinWaitCondition(t, nil, func() bool {
				return atomic.LoadInt64(&r.atomicRestBufferSizeBytes) != old
			})
		}

		xtest.TestManyTimesWithName(t, "InitialBufferSize", func(t testing.TB) {
			e := newTopicReaderTestEnv(t)
			e.Start()
			waitChangeRestBufferSizeBytes(e.reader, 0)
			require.Equal(t, e.initialBufferSizeBytes, e.reader.atomicRestBufferSizeBytes)
		})

		xtest.TestManyTimesWithName(t, "DecrementIncrementBufferSize", func(t testing.TB) {
			e := newTopicReaderTestEnv(t)

			// doesn't check sends
			e.stream.EXPECT().Send(gomock.Any()).Return(nil).MinTimes(1)

			e.Start()
			waitChangeRestBufferSizeBytes(e.reader, 0)

			const dataSize = 1000
			e.SendFromServer(&rawtopicreader.ReadResponse{BytesSize: dataSize, PartitionData: []rawtopicreader.PartitionData{
				{
					PartitionSessionID: e.partitionSessionID,
					Batches: []rawtopicreader.Batch{
						{
							MessageData: []rawtopicreader.MessageData{
								{
									Offset: 1,
									SeqNo:  1,
									Data:   []byte{1, 2},
								},
								{
									Offset: 2,
									SeqNo:  2,
									Data:   []byte{4, 5, 6},
								},
								{
									Offset: 3,
									SeqNo:  3,
									Data:   []byte{7},
								},
							},
						},
					},
				},
			}})
			waitChangeRestBufferSizeBytes(e.reader, e.initialBufferSizeBytes)
			expectedBufferSizeAfterReceiveMessages := e.initialBufferSizeBytes - dataSize
			require.Equal(t, e.reader.atomicRestBufferSizeBytes, expectedBufferSizeAfterReceiveMessages)

			oneOption := newReadMessageBatchOptions()
			oneOption.MaxCount = 1
			_, err := e.reader.ReadMessageBatch(e.ctx, oneOption)
			require.NoError(t, err)

			waitChangeRestBufferSizeBytes(e.reader, expectedBufferSizeAfterReceiveMessages)

			bufferSizeAfterReadOneMessage := e.reader.atomicRestBufferSizeBytes

			_, err = e.reader.ReadMessageBatch(e.ctx, newReadMessageBatchOptions())
			require.NoError(t, err)

			waitChangeRestBufferSizeBytes(e.reader, bufferSizeAfterReadOneMessage)
			require.Equal(t, e.initialBufferSizeBytes, e.reader.atomicRestBufferSizeBytes)
		})

		xtest.TestManyTimesWithName(t, "ForceReturnBatchIfBufferFull", func(t testing.TB) {
			e := newTopicReaderTestEnv(t)

			dataRequested := make(empty.Chan)
			e.stream.EXPECT().Send(&rawtopicreader.ReadRequest{BytesSize: int(e.initialBufferSizeBytes)}).
				Do(func(_ interface{}) {
					close(dataRequested)
				})

			e.Start()
			waitChangeRestBufferSizeBytes(e.reader, 0)

			e.SendFromServer(&rawtopicreader.ReadResponse{
				BytesSize: int(e.initialBufferSizeBytes),
				PartitionData: []rawtopicreader.PartitionData{
					{
						PartitionSessionID: e.partitionSessionID,
						Batches: []rawtopicreader.Batch{
							{
								MessageData: []rawtopicreader.MessageData{
									{
										Offset: 1,
										SeqNo:  1,
										Data:   []byte{1, 2, 3},
									},
								},
							},
						},
					},
				},
			})
			needReadTwoMessages := newReadMessageBatchOptions()
			needReadTwoMessages.MinCount = 2

			readTimeoutCtx, cancel := context.WithTimeout(e.ctx, time.Second)
			defer cancel()

			batch, err := e.reader.ReadMessageBatch(readTimeoutCtx, needReadTwoMessages)
			require.NoError(t, err)
			require.Len(t, batch.Messages, 1)

			<-dataRequested
		})
	})

	xtest.TestManyTimesWithName(t, "ReadBatch", func(t testing.TB) {
		e := newTopicReaderTestEnv(t)
		e.Start()

		compress := func(msg string) []byte {
			b := &bytes.Buffer{}
			writer := gzip.NewWriter(b)
			_, err := writer.Write([]byte(msg))
			require.NoError(t, writer.Close())
			require.NoError(t, err)
			return b.Bytes()
		}

		prevOffset := e.partitionSession.lastReceivedMessageOffset()

		sendDataRequestCompleted := make(empty.Chan)
		dataSize := 4
		e.stream.EXPECT().Send(&rawtopicreader.ReadRequest{BytesSize: dataSize}).Do(func(_ interface{}) {
			close(sendDataRequestCompleted)
		})
		e.SendFromServer(&rawtopicreader.ReadResponse{
			BytesSize: dataSize,
			PartitionData: []rawtopicreader.PartitionData{
				{
					PartitionSessionID: e.partitionSessionID,
					Batches: []rawtopicreader.Batch{
						{
							Codec:            rawtopiccommon.CodecRaw,
							WriteSessionMeta: map[string]string{"a": "b", "c": "d"},
							WrittenAt:        testTime(5),
							MessageData: []rawtopicreader.MessageData{
								{
									Offset:           prevOffset + 1,
									SeqNo:            1,
									CreatedAt:        testTime(1),
									Data:             []byte("123"),
									UncompressedSize: 3,
									MessageGroupID:   "1",
								},
								{
									Offset:           prevOffset + 2,
									SeqNo:            2,
									CreatedAt:        testTime(2),
									Data:             []byte("4567"),
									UncompressedSize: 4,
									MessageGroupID:   "1",
								},
							},
						},
						{
							Codec:            rawtopiccommon.CodecGzip,
							WriteSessionMeta: map[string]string{"e": "f", "g": "h"},
							WrittenAt:        testTime(6),
							MessageData: []rawtopicreader.MessageData{
								{
									Offset:           prevOffset + 10,
									SeqNo:            3,
									CreatedAt:        testTime(3),
									Data:             compress("098"),
									UncompressedSize: 3,
									MessageGroupID:   "2",
								},
								{
									Offset:           prevOffset + 20,
									SeqNo:            4,
									CreatedAt:        testTime(4),
									Data:             compress("0987"),
									UncompressedSize: 4,
									MessageGroupID:   "2",
								},
							},
						},
					},
				},
			},
		},
		)

		expectedData := [][]byte{[]byte("123"), []byte("4567"), []byte("098"), []byte("0987")}
		expectedBatch := &PublicBatch{
			commitRange: commitRange{
				commitOffsetStart: prevOffset + 1,
				commitOffsetEnd:   prevOffset + 21,
				partitionSession:  e.partitionSession,
			},
			Messages: []*PublicMessage{
				{
					SeqNo:                1,
					CreatedAt:            testTime(1),
					MessageGroupID:       "1",
					Offset:               prevOffset.ToInt64() + 1,
					WrittenAt:            testTime(5),
					WriteSessionMetadata: map[string]string{"a": "b", "c": "d"},
					UncompressedSize:     3,
					rawDataLen:           3,
					commitRange: commitRange{
						commitOffsetStart: prevOffset + 1,
						commitOffsetEnd:   prevOffset + 2,
						partitionSession:  e.partitionSession,
					},
				},
				{
					SeqNo:                2,
					CreatedAt:            testTime(2),
					MessageGroupID:       "1",
					Offset:               prevOffset.ToInt64() + 2,
					WrittenAt:            testTime(5),
					WriteSessionMetadata: map[string]string{"a": "b", "c": "d"},
					rawDataLen:           4,
					UncompressedSize:     4,
					commitRange: commitRange{
						commitOffsetStart: prevOffset + 2,
						commitOffsetEnd:   prevOffset + 3,
						partitionSession:  e.partitionSession,
					},
				},
				{
					SeqNo:                3,
					CreatedAt:            testTime(3),
					MessageGroupID:       "2",
					Offset:               prevOffset.ToInt64() + 10,
					WrittenAt:            testTime(6),
					WriteSessionMetadata: map[string]string{"e": "f", "g": "h"},
					rawDataLen:           len(compress("098")),
					UncompressedSize:     3,
					commitRange: commitRange{
						commitOffsetStart: prevOffset + 3,
						commitOffsetEnd:   prevOffset + 11,
						partitionSession:  e.partitionSession,
					},
				},
				{
					SeqNo:                4,
					CreatedAt:            testTime(4),
					MessageGroupID:       "2",
					Offset:               prevOffset.ToInt64() + 20,
					WrittenAt:            testTime(6),
					WriteSessionMetadata: map[string]string{"e": "f", "g": "h"},
					rawDataLen:           len(compress("0987")),
					UncompressedSize:     4,
					commitRange: commitRange{
						commitOffsetStart: prevOffset + 11,
						commitOffsetEnd:   prevOffset + 21,
						partitionSession:  e.partitionSession,
					},
				},
			},
		}

		opts := newReadMessageBatchOptions()
		opts.MinCount = 4
		batch, err := e.reader.ReadMessageBatch(e.ctx, opts)
		require.NoError(t, err)

		data := make([][]byte, 0, len(batch.Messages))
		for i := range batch.Messages {
			content, err := ioutil.ReadAll(&batch.Messages[i].data)
			require.NoError(t, err)
			data = append(data, content)
			batch.Messages[i].data = newOneTimeReader(nil)
			batch.Messages[i].bufferBytesAccount = 0
		}

		require.Equal(t, expectedData, data)
		require.Equal(t, expectedBatch, batch)
		<-sendDataRequestCompleted
	})
}

func TestTopicStreamReadImpl_BatchReaderWantMoreMessagesThenBufferCanHold(t *testing.T) {
	sendMessageWithFullBuffer := func(e *streamEnv) empty.Chan {
		nextDataRequested := make(empty.Chan)
		e.stream.EXPECT().Send(&rawtopicreader.ReadRequest{BytesSize: int(e.initialBufferSizeBytes)}).Do(func(_ interface{}) {
			close(nextDataRequested)
		})

		e.SendFromServer(
			&rawtopicreader.ReadResponse{
				BytesSize: int(e.initialBufferSizeBytes),
				PartitionData: []rawtopicreader.PartitionData{
					{
						PartitionSessionID: e.partitionSessionID,
						Batches: []rawtopicreader.Batch{
							{
								Codec: rawtopiccommon.CodecRaw,
								MessageData: []rawtopicreader.MessageData{
									{
										Offset: 1,
									},
								},
							},
						},
					},
				},
			})
		return nextDataRequested
	}

	xtest.TestManyTimesWithName(t, "ReadAfterMessageInBuffer", func(t testing.TB) {
		e := newTopicReaderTestEnv(t)
		e.Start()

		nextDataRequested := sendMessageWithFullBuffer(&e)

		// wait message received to internal buffer
		xtest.SpinWaitCondition(t, &e.reader.batcher.m, func() bool {
			return len(e.reader.batcher.messages) > 0
		})

		xtest.SpinWaitCondition(t, nil, func() bool {
			return atomic.LoadInt64(&e.reader.atomicRestBufferSizeBytes) == 0
		})

		opts := newReadMessageBatchOptions()
		opts.MinCount = 2

		readCtx, cancel := context.WithTimeout(e.ctx, time.Second)
		defer cancel()
		batch, err := e.reader.ReadMessageBatch(readCtx, opts)
		require.NoError(t, err)
		require.Len(t, batch.Messages, 1)
		require.Equal(t, int64(1), batch.Messages[0].Offset)

		<-nextDataRequested
		require.Equal(t, e.initialBufferSizeBytes, atomic.LoadInt64(&e.reader.atomicRestBufferSizeBytes))
	})

	xtest.TestManyTimesWithName(t, "ReadBeforeMessageInBuffer", func(t testing.TB) {
		e := newTopicReaderTestEnv(t)
		e.Start()

		readCompleted := make(empty.Chan)
		var batch *PublicBatch
		var readErr error
		go func() {
			defer close(readCompleted)

			opts := newReadMessageBatchOptions()
			opts.MinCount = 2

			readCtx, cancel := context.WithTimeout(e.ctx, time.Second)
			defer cancel()
			batch, readErr = e.reader.ReadMessageBatch(readCtx, opts)
		}()

		// wait to start pop
		e.reader.batcher.notifyAboutNewMessages()
		xtest.SpinWaitCondition(t, &e.reader.batcher.m, func() bool {
			return len(e.reader.batcher.hasNewMessages) == 0
		})

		nextDataRequested := sendMessageWithFullBuffer(&e)

		<-readCompleted
		require.NoError(t, readErr)
		require.Len(t, batch.Messages, 1)
		require.Equal(t, int64(1), batch.Messages[0].Offset)

		<-nextDataRequested
		require.Equal(t, e.initialBufferSizeBytes, atomic.LoadInt64(&e.reader.atomicRestBufferSizeBytes))
	})
}

type streamEnv struct {
	ctx                    context.Context
	t                      testing.TB
	reader                 *topicStreamReaderImpl
	stopReadEvents         empty.Chan
	stream                 *MockRawTopicReaderStream
	partitionSessionID     partitionSessionID
	mc                     *gomock.Controller
	partitionSession       *partitionSession
	initialBufferSizeBytes int64

	m                          xsync.Mutex
	messagesFromServerToClient chan testStreamResult
	nextMessageNeedCallback    func()
}

type testStreamResult struct {
	nextMessageCallback func()
	msg                 rawtopicreader.ServerMessage
	err                 error
	waitOnly            bool
}

func newTopicReaderTestEnv(t testing.TB) streamEnv {
	ctx := testContext(t)

	mc := gomock.NewController(t)

	stream := NewMockRawTopicReaderStream(mc)

	const initialBufferSizeBytes = 1000000

	cfg := newTopicStreamReaderConfig()
	cfg.BaseContext = ctx
	cfg.BufferSizeProtoBytes = initialBufferSizeBytes

	reader := newTopicStreamReaderStopped(stream, cfg)
	// reader.initSession() - skip stream level initialization

	const testPartitionID = 5
	const testSessionID = 15
	const testSessionComitted = 20

	session := newPartitionSession(ctx, "/test", testPartitionID, testSessionID, testSessionComitted)
	require.NoError(t, reader.sessionController.Add(session))

	env := streamEnv{
		ctx:                        ctx,
		t:                          t,
		initialBufferSizeBytes:     initialBufferSizeBytes,
		reader:                     reader,
		stopReadEvents:             make(empty.Chan),
		stream:                     stream,
		messagesFromServerToClient: make(chan testStreamResult),
		partitionSession:           session,
		partitionSessionID:         session.partitionSessionID,
		mc:                         mc,
	}

	stream.EXPECT().Recv().AnyTimes().DoAndReturn(env.receiveMessageHandler)

	// initial data request
	stream.EXPECT().Send(&rawtopicreader.ReadRequest{BytesSize: initialBufferSizeBytes})

	// allow in test send data without explicit sizes
	stream.EXPECT().Send(&rawtopicreader.ReadRequest{BytesSize: 0}).AnyTimes()

	streamClosed := make(empty.Chan)
	stream.EXPECT().CloseSend().Return(nil).Do(func() {
		close(streamClosed)
	})

	t.Cleanup(func() {
		cleanupTimeout, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		close(env.stopReadEvents)
		_ = env.reader.CloseWithError(ctx, errors.New("test finished"))
		require.NoError(t, cleanupTimeout.Err())
		xtest.WaitChannelClosed(t, streamClosed)
	})

	t.Cleanup(func() {
		if messLen := len(env.messagesFromServerToClient); messLen != 0 {
			t.Fatalf("not all messages consumed from server: %v", messLen)
		}
	})

	//nolint:govet
	return env
}

func (e *streamEnv) Start() {
	require.NoError(e.t, e.reader.startLoops())
	xtest.SpinWaitCondition(e.t, nil, func() bool {
		return atomic.LoadInt64(&e.reader.atomicRestBufferSizeBytes) == e.initialBufferSizeBytes
	})
}

func (e *streamEnv) readerReceiveWaitClose(callback func()) {
	e.stream.EXPECT().Recv().Do(func() {
		if callback != nil {
			callback()
		}
		<-e.ctx.Done()
	}).Return(nil, errors.New("test reader closed"))
}

func (e *streamEnv) SendFromServer(msg rawtopicreader.ServerMessage) {
	e.SendFromServerAndSetNextCallback(msg, nil)
}

func (e *streamEnv) SendFromServerAndSetNextCallback(msg rawtopicreader.ServerMessage, callback func()) {
	if msg.StatusData().Status == 0 {
		msg.SetStatus(rawydb.StatusSuccess)
	}
	e.messagesFromServerToClient <- testStreamResult{msg: msg, nextMessageCallback: callback}
}

func (e *streamEnv) WaitMessageReceived() {
	e.messagesFromServerToClient <- testStreamResult{waitOnly: true}
}

func (e *streamEnv) receiveMessageHandler() (rawtopicreader.ServerMessage, error) {
	if e.ctx.Err() != nil {
		return nil, e.ctx.Err()
	}

	var callback func()
	e.m.WithLock(func() {
		callback = e.nextMessageNeedCallback
		e.nextMessageNeedCallback = nil
	})

	if callback != nil {
		callback()
	}

readMessages:
	for {
		select {
		case <-e.ctx.Done():
			return nil, e.ctx.Err()
		case <-e.stopReadEvents:
			return nil, xerrors.Wrap(errors.New("mock reader closed"))
		case res := <-e.messagesFromServerToClient:
			if res.waitOnly {
				continue readMessages
			}
			e.m.WithLock(func() {
				e.nextMessageNeedCallback = res.nextMessageCallback
			})
			return res.msg, res.err
		}
	}
}

func testContext(t testing.TB) context.Context {
	ctx, cancel := xcontext.WithErrCancel(context.Background())
	ctx = pprof.WithLabels(ctx, pprof.Labels("test", t.Name()))
	pprof.SetGoroutineLabels(ctx)

	t.Cleanup(func() {
		pprof.SetGoroutineLabels(ctx)
		cancel(fmt.Errorf("test context finished: %v", t.Name()))
	})
	return ctx
}
