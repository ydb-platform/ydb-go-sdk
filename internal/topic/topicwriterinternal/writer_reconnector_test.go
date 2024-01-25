package topicwriterinternal

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xatomic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

var testCommonEncoders = NewEncoderMap()

func TestWriterImpl_AutoSeq(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		ctx := xtest.Context(t)
		w := newWriterReconnectorStopped(newWriterReconnectorConfig(
			WithAutoSetSeqNo(true),
			WithAutosetCreatedTime(false),
		))
		w.firstConnectionHandled.Store(true)

		lastSeqNo := int64(16)
		w.lastSeqNo = lastSeqNo

		var wg sync.WaitGroup
		fWrite := func(num int) {
			defer wg.Done()

			msgs := newTestMessages(0)
			msgs[0].CreatedAt = time.Unix(int64(num), 0)
			require.NoError(t, w.Write(ctx, msgs))
		}

		const messCount = 1000
		wg.Add(messCount)
		for i := 0; i < messCount; i++ {
			go fWrite(i)
		}
		wg.Wait()

		require.Len(t, w.queue.messagesByOrder, messCount)
		require.Equal(t, lastSeqNo+messCount, w.queue.lastSeqNo)
	})

	t.Run("PredefinedSeqNo", func(t *testing.T) {
		ctx := xtest.Context(t)

		w := newWriterReconnectorStopped(newWriterReconnectorConfig(WithAutoSetSeqNo(true)))
		w.firstConnectionHandled.Store(true)
		require.Error(t, w.Write(ctx, newTestMessages(1)))
	})
}

func TestWriterImpl_CheckMessages(t *testing.T) {
	t.Run("MessageSize", func(t *testing.T) {
		ctx := xtest.Context(t)
		w := newWriterReconnectorStopped(newWriterReconnectorConfig())
		w.firstConnectionHandled.Store(true)

		maxSize := 5
		w.cfg.MaxMessageSize = maxSize

		err := w.Write(ctx, []PublicMessage{{Data: bytes.NewReader(make([]byte, maxSize))}})
		require.NoError(t, err)

		err = w.Write(ctx, []PublicMessage{{Data: bytes.NewReader(make([]byte, maxSize+1))}})
		require.Error(t, err)
	})
}

func TestWriterImpl_Write(t *testing.T) {
	t.Run("PushToQueue", func(t *testing.T) {
		ctx := context.Background()
		w := newTestWriterStopped()
		w.cfg.AutoSetCreatedTime = false
		w.firstConnectionHandled.Store(true)

		err := w.Write(ctx, newTestMessages(1, 3, 5))
		require.NoError(t, err)

		expectedMap := map[int]messageWithDataContent{
			1: newTestMessageWithDataContent(1),
			2: newTestMessageWithDataContent(3),
			3: newTestMessageWithDataContent(5),
		}

		for k := range expectedMap {
			mess := expectedMap[k]
			_, err = mess.GetEncodedBytes(rawtopiccommon.CodecRaw)
			require.NoError(t, err)
			mess.metadataCached = true
			expectedMap[k] = mess
		}

		require.Equal(t, expectedMap, w.queue.messagesByOrder)
	})
	t.Run("WriteWithSyncMode", func(t *testing.T) {
		xtest.TestManyTimes(t, func(t testing.TB) {
			e := newTestEnv(t, &testEnvOptions{
				writerOptions: []PublicWriterOption{
					WithWaitAckOnWrite(true),
				},
			})

			messageTime := time.Date(2022, 9, 7, 11, 34, 0, 0, time.UTC)
			messageData := []byte("123")

			const seqNo = 31

			writeMessageReceived := make(empty.Chan)
			e.stream.EXPECT().Send(&rawtopicwriter.WriteRequest{
				Messages: []rawtopicwriter.MessageData{
					{
						SeqNo:            seqNo,
						CreatedAt:        messageTime,
						UncompressedSize: int64(len(messageData)),
						Partitioning:     rawtopicwriter.Partitioning{},
						Data:             messageData,
					},
				},
				Codec: rawtopiccommon.CodecRaw,
			}).Do(func(_ interface{}) {
				close(writeMessageReceived)
			}).Return(nil)

			writeCompleted := make(empty.Chan)
			go func() {
				err := e.writer.Write(e.ctx, []PublicMessage{{
					SeqNo:     seqNo,
					CreatedAt: messageTime,
					Data:      bytes.NewReader(messageData),
				}})
				require.NoError(t, err)
				close(writeCompleted)
			}()

			<-writeMessageReceived

			select {
			case <-writeCompleted:
				t.Fatal("sync write must complete after receive ack only")
			default:
				// pass
			}

			e.sendFromServer(&rawtopicwriter.WriteResult{
				Acks: []rawtopicwriter.WriteAck{
					{
						SeqNo: seqNo,
						MessageWriteStatus: rawtopicwriter.MessageWriteStatus{
							Type:          rawtopicwriter.WriteStatusTypeWritten,
							WrittenOffset: 4,
						},
					},
				},
				PartitionID: e.partitionID,
			})

			xtest.WaitChannelClosed(t, writeCompleted)
		})
	})
}

func TestWriterImpl_WriteCodecs(t *testing.T) {
	t.Run("ForceRaw", func(t *testing.T) {
		var err error
		e := newTestEnv(t, &testEnvOptions{writerOptions: []PublicWriterOption{WithCodec(rawtopiccommon.CodecRaw)}})

		messContent := []byte("123")

		messReceived := make(chan rawtopiccommon.Codec, 2)
		e.stream.EXPECT().Send(gomock.Any()).Do(func(message rawtopicwriter.ClientMessage) {
			writeReq := message.(*rawtopicwriter.WriteRequest)
			messReceived <- writeReq.Codec
		})

		require.NoError(t, err)
		require.NoError(t, e.writer.Write(e.ctx, []PublicMessage{{
			Data: bytes.NewReader(messContent),
		}}))

		require.Equal(t, rawtopiccommon.CodecRaw, <-messReceived)
	})
	t.Run("ForceGzip", func(t *testing.T) {
		var err error
		e := newTestEnv(t, &testEnvOptions{
			writerOptions: []PublicWriterOption{WithCodec(rawtopiccommon.CodecGzip)},
			topicCodecs:   rawtopiccommon.SupportedCodecs{rawtopiccommon.CodecGzip},
		})

		messContent := []byte("123")

		gzipped := &bytes.Buffer{}
		writer := gzip.NewWriter(gzipped)
		_, err = writer.Write(messContent)
		require.NoError(t, err)
		require.NoError(t, writer.Close())

		messReceived := make(chan rawtopiccommon.Codec, 2)
		e.stream.EXPECT().Send(gomock.Any()).Do(func(message rawtopicwriter.ClientMessage) {
			writeReq := message.(*rawtopicwriter.WriteRequest)
			messReceived <- writeReq.Codec
		})

		require.NoError(t, err)
		require.NoError(t, e.writer.Write(e.ctx, []PublicMessage{{
			Data: bytes.NewReader(messContent),
		}}))

		require.Equal(t, rawtopiccommon.CodecGzip, <-messReceived)
	})
	t.Run("Auto", func(t *testing.T) {
		e := newTestEnv(t, &testEnvOptions{
			writerOptions: []PublicWriterOption{
				WithAutoSetSeqNo(true),
				WithAutoCodec(),
			},
			topicCodecs: rawtopiccommon.SupportedCodecs{rawtopiccommon.CodecRaw, rawtopiccommon.CodecGzip},
		})

		messContentShort := []byte("1")
		messContentLong := make([]byte, 100000)

		messReceived := make(chan rawtopiccommon.Codec, 2)
		e.stream.EXPECT().Send(gomock.Any()).Do(func(message rawtopicwriter.ClientMessage) {
			writeReq := message.(*rawtopicwriter.WriteRequest)
			messReceived <- writeReq.Codec
		}).Times(codecMeasureIntervalBatches * 2)

		codecs := make(map[rawtopiccommon.Codec]empty.Struct)

		for i := 0; i < codecMeasureIntervalBatches; i++ {
			require.NoError(t, e.writer.Write(e.ctx, []PublicMessage{{
				Data: bytes.NewReader(messContentShort),
			}}))
			// wait send
			codec := <-messReceived
			codecs[codec] = empty.Struct{}
		}

		for i := 0; i < codecMeasureIntervalBatches; i++ {
			require.NoError(t, e.writer.Write(e.ctx, []PublicMessage{{
				Data: bytes.NewReader(messContentLong),
			}}))
			// wait send
			codec := <-messReceived
			codecs[codec] = empty.Struct{}
		}

		// used two different codecs
		require.Len(t, codecs, 2)
	})
}

func TestWriterReconnector_Write_QueueLimit(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctx := xtest.Context(t)
		w := newWriterReconnectorStopped(newWriterReconnectorConfig(
			WithAutoSetSeqNo(false),
			WithMaxQueueLen(2),
		))
		w.firstConnectionHandled.Store(true)

		waitStartQueueWait := func(targetWaiters int) {
			xtest.SpinWaitCondition(t, nil, func() bool {
				res := getWaitersCount(w.semaphore) == targetWaiters
				return res
			})
		}

		err := w.Write(ctx, newTestMessages(1, 2))
		require.NoError(t, err)

		ctxNoQueueSpace, ctxNoQueueSpaceCancel := xcontext.WithCancel(ctx)

		go func() {
			waitStartQueueWait(1)
			ctxNoQueueSpaceCancel()
		}()
		err = w.Write(ctxNoQueueSpace, newTestMessages(3))
		if !errors.Is(err, PublicErrQueueIsFull) {
			require.ErrorIs(t, err, PublicErrQueueIsFull)
		}

		go func() {
			waitStartQueueWait(1)
			ackErr := w.queue.AcksReceived([]rawtopicwriter.WriteAck{
				{
					SeqNo: 1,
				},
			})
			require.NoError(t, ackErr)
		}()

		err = w.Write(ctx, newTestMessages(3))
		require.NoError(t, err)
	})
}

func TestEnv(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		env := newTestEnv(t, nil)
		xtest.WaitChannelClosed(t, env.writer.firstInitResponseProcessedChan)
	})
}

func TestWriterImpl_InitSession(t *testing.T) {
	w := newTestWriterStopped(WithAutoSetSeqNo(true))
	lastSeqNo := int64(123)
	sessionID := "test-session-id"

	w.onWriterChange(&SingleStreamWriter{
		ReceivedLastSeqNum: lastSeqNo,
		SessionID:          sessionID,
	})

	require.Equal(t, sessionID, w.sessionID)
	require.Equal(t, lastSeqNo, w.lastSeqNo)
	require.True(t, isClosed(w.firstInitResponseProcessedChan))
}

func TestWriterImpl_WaitInit(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		w := newTestWriterStopped(WithAutoSetSeqNo(true))
		expectedInitData := InitialInfo{
			LastSeqNum: int64(123),
		}
		w.onWriterChange(&SingleStreamWriter{
			ReceivedLastSeqNum: expectedInitData.LastSeqNum,
		})

		initData, err := w.WaitInit(context.Background())
		require.NoError(t, err)
		require.Equal(t, expectedInitData, initData)

		err = w.Write(context.Background(), newTestMessages(0))
		require.NoError(t, err)

		// one more run is needed to check idempotency
		anotherInitData, err := w.WaitInit(context.Background())
		require.NoError(t, err)
		require.Equal(t, initData, anotherInitData)

		require.True(t, isClosed(w.firstInitResponseProcessedChan))
	})

	t.Run("contextDeadlineErrorInProgress", func(t *testing.T) {
		w := newTestWriterStopped(WithAutoSetSeqNo(true))
		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			// wait until w.WaitInit starts
			time.Sleep(time.Millisecond)
			cancel()
		}()

		_, err := w.WaitInit(ctx)
		require.ErrorIs(t, err, ctx.Err())
	})

	t.Run("contextDeadlineErrorBeforeStart", func(t *testing.T) {
		w := newTestWriterStopped(WithAutoSetSeqNo(true))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := w.WaitInit(ctx)
		require.ErrorIs(t, err, ctx.Err())

		w.onWriterChange(&SingleStreamWriter{})
		require.True(t, isClosed(w.firstInitResponseProcessedChan))
	})
}

func TestWriterImpl_Reconnect(t *testing.T) {
	t.Run("StopReconnectOnUnretryableError", func(t *testing.T) {
		mc := gomock.NewController(t)
		strm := NewMockRawTopicWriterStream(mc)

		w := newTestWriterStopped()

		ctx := xtest.Context(t)
		testErr := errors.New("test")

		connectCalled := false
		connectCalledChan := make(empty.Chan)

		w.cfg.Connect = func(streamCtxArg context.Context) (RawTopicWriterStream, error) {
			close(connectCalledChan)
			connectCalled = true
			require.NotEqual(t, ctx, streamCtxArg)
			return strm, nil
		}

		initRequest := testCreateInitRequest(w)
		strm.EXPECT().Send(&initRequest)
		strm.EXPECT().Recv().Return(nil, testErr)
		strm.EXPECT().CloseSend()

		w.connectionLoop(ctx)

		require.True(t, connectCalled)
		require.ErrorIs(t, w.background.CloseReason(), testErr)
	})

	xtest.TestManyTimesWithName(t, "ReconnectOnErrors", func(t testing.TB) {
		ctx := xtest.Context(t)

		w := newTestWriterStopped()

		mc := gomock.NewController(t)

		type connectionAttemptContext struct {
			name            string
			stream          RawTopicWriterStream
			connectionError error
		}

		newStream := func(name string) *MockRawTopicWriterStream {
			strm := NewMockRawTopicWriterStream(mc)
			initReq := testCreateInitRequest(w)

			streamClosed := make(empty.Chan)
			strm.EXPECT().CloseSend().Do(func() {
				t.Logf("closed stream: %v", name)
				close(streamClosed)
			})

			strm.EXPECT().Send(&initReq).Do(func(_ interface{}) {
				t.Logf("sent init request stream: %v", name)
			})

			strm.EXPECT().Recv().Do(func() {
				t.Logf("receive init response stream: %v", name)
			}).Return(&rawtopicwriter.InitResult{
				ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{Status: rawydb.StatusSuccess},
				SessionID:             name,
			}, nil)

			strm.EXPECT().Recv().Do(func() {
				t.Logf("waiting close channel: %v", name)
				xtest.WaitChannelClosed(t, streamClosed)
				t.Logf("channel closed: %v", name)
			}).Return(nil, errors.New("test stream closed")).MaxTimes(1)
			return strm
		}

		strm2 := newStream("strm2")
		strm2.EXPECT().Send(&rawtopicwriter.WriteRequest{
			Messages: []rawtopicwriter.MessageData{
				{SeqNo: 1},
			},
			Codec: rawtopiccommon.CodecRaw,
		}).Do(func(_ *rawtopicwriter.WriteRequest) {
			t.Logf("strm2 sent message and return retriable error")
		}).Return(xerrors.Retryable(errors.New("retriable on strm2")))

		strm3 := newStream("strm3")
		strm3.EXPECT().Send(&rawtopicwriter.WriteRequest{
			Messages: []rawtopicwriter.MessageData{
				{SeqNo: 1},
			},
			Codec: rawtopiccommon.CodecRaw,
		}).Do(func(_ *rawtopicwriter.WriteRequest) {
			t.Logf("strm3 sent message and return unretriable error")
		}).Return(errors.New("strm3"))

		connectsResult := []connectionAttemptContext{
			{
				name:            "step-1 connection error",
				stream:          nil,
				connectionError: xerrors.Retryable(errors.New("test-1")),
			},
			{
				name:   "step-2 connect and return retryable error on write",
				stream: strm2,
			},
			{
				name:   "step-3 connect and return unretriable error on write",
				stream: strm3,
			},
		}

		var connectionAttempt xatomic.Int64
		w.cfg.Connect = func(ctx context.Context) (RawTopicWriterStream, error) {
			attemptIndex := int(connectionAttempt.Add(1)) - 1
			t.Logf("connect with attempt index: %v", attemptIndex)
			res := connectsResult[attemptIndex]

			return res.stream, res.connectionError
		}

		connectionLoopStopped := make(empty.Chan)
		go func() {
			defer close(connectionLoopStopped)
			w.connectionLoop(ctx)
			t.Log("connection loop stopped")
		}()

		err := w.Write(ctx, newTestMessages(1))
		require.NoError(t, err)

		xtest.WaitChannelClosed(t, connectionLoopStopped)
	})
}

func TestAllMessagesHasSameBufCodec(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		require.True(t, allMessagesHasSameBufCodec(nil))
	})

	t.Run("One", func(t *testing.T) {
		require.True(t, allMessagesHasSameBufCodec(newTestMessagesWithContent(1)))
	})

	t.Run("SameCodecs", func(t *testing.T) {
		require.True(t, allMessagesHasSameBufCodec(newTestMessagesWithContent(1, 2, 3)))
	})
	t.Run("DifferCodecs", func(t *testing.T) {
		for i := 0; i < 3; i++ {
			messages := newTestMessagesWithContent(1, 2, 3)
			messages[i].bufCodec = rawtopiccommon.CodecGzip
			require.False(t, allMessagesHasSameBufCodec(messages))
		}
	})
}

func TestCreateRawMessageData(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		req, err := createWriteRequest(newTestMessagesWithContent(), rawtopiccommon.CodecRaw)
		require.NoError(t, err)
		require.Equal(t,
			rawtopicwriter.WriteRequest{
				Messages: []rawtopicwriter.MessageData{},
				Codec:    rawtopiccommon.CodecRaw,
			},
			req,
		)
	})
	t.Run("WithMessageMetadata", func(t *testing.T) {
		messages := newTestMessagesWithContent(1)
		messages[0].Metadata = map[string][]byte{
			"a": {1, 2, 3},
			"b": {4, 5},
		}
		req, err := createWriteRequest(messages, rawtopiccommon.CodecRaw)

		sort.Slice(req.Messages[0].MetadataItems, func(i, j int) bool {
			return req.Messages[0].MetadataItems[i].Key < req.Messages[0].MetadataItems[j].Key
		})

		require.NoError(t, err)
		require.Equal(t, rawtopicwriter.WriteRequest{
			Messages: []rawtopicwriter.MessageData{
				{
					SeqNo: 1,
					MetadataItems: []rawtopiccommon.MetadataItem{
						{
							Key:   "a",
							Value: []byte{1, 2, 3},
						},
						{
							Key:   "b",
							Value: []byte{4, 5},
						},
					},
				},
			},
			Codec: rawtopiccommon.CodecRaw,
		}, req)
	})
	t.Run("WithSeqno", func(t *testing.T) {
		req, err := createWriteRequest(newTestMessagesWithContent(1, 2, 3), rawtopiccommon.CodecRaw)
		require.NoError(t, err)
		require.Equal(t,
			rawtopicwriter.WriteRequest{
				Messages: []rawtopicwriter.MessageData{
					{
						SeqNo: 1,
					},
					{
						SeqNo: 2,
					},
					{
						SeqNo: 3,
					},
				},
				Codec: rawtopiccommon.CodecRaw,
			},
			req,
		)
	})
}

func TestSplitMessagesByBufCodec(t *testing.T) {
	tests := [][]rawtopiccommon.Codec{
		nil,
		{},
		{rawtopiccommon.CodecRaw},
		{rawtopiccommon.CodecRaw, rawtopiccommon.CodecRaw},
		{rawtopiccommon.CodecRaw, rawtopiccommon.CodecGzip},
		{
			rawtopiccommon.CodecRaw,
			rawtopiccommon.CodecGzip,
			rawtopiccommon.CodecGzip,
			rawtopiccommon.CodecRaw,
			rawtopiccommon.CodecGzip,
			rawtopiccommon.CodecRaw,
			rawtopiccommon.CodecRaw,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprint(test), func(t *testing.T) {
			var messages []messageWithDataContent
			for index, codec := range test {
				mess := newTestMessageWithDataContent(index)
				mess.bufCodec = codec
				messages = append(messages, mess)
			}

			groups := splitMessagesByBufCodec(messages)
			expectedNum := int64(-1)
			for _, group := range groups {
				require.NotEmpty(t, group)
				require.True(t, allMessagesHasSameBufCodec(group))
				require.Len(t, group, cap(group))
				for _, mess := range group {
					expectedNum++
					require.Equal(t, test[int(expectedNum)], mess.bufCodec)
					mess.SeqNo = expectedNum
				}
			}

			require.Equal(t, int(expectedNum), len(test)-1)
		})
	}
}

func TestCalculateAllowedCodecs(t *testing.T) {
	customCodecSupported := rawtopiccommon.Codec(rawtopiccommon.CodecCustomerFirst)
	customCodecUnsupported := rawtopiccommon.Codec(rawtopiccommon.CodecCustomerFirst + 1)
	encoders := NewEncoderMap()
	encoders.AddEncoder(customCodecSupported, func(writer io.Writer) (io.WriteCloser, error) {
		return nil, errors.New("test")
	})

	table := []struct {
		name           string
		force          rawtopiccommon.Codec
		serverCodecs   rawtopiccommon.SupportedCodecs
		expectedResult rawtopiccommon.SupportedCodecs
	}{
		{
			name:         "ForceRawWithEmptyServer",
			force:        rawtopiccommon.CodecRaw,
			serverCodecs: nil,
			expectedResult: rawtopiccommon.SupportedCodecs{
				rawtopiccommon.CodecRaw,
			},
		},
		{
			name:  "ForceRawWithAllowedByServer",
			force: rawtopiccommon.CodecRaw,
			serverCodecs: rawtopiccommon.SupportedCodecs{
				rawtopiccommon.CodecRaw,
				rawtopiccommon.CodecGzip,
			},
			expectedResult: rawtopiccommon.SupportedCodecs{
				rawtopiccommon.CodecRaw,
			},
		},
		{
			name:  "ForceCustomWithAllowedByServer",
			force: customCodecSupported,
			serverCodecs: rawtopiccommon.SupportedCodecs{
				rawtopiccommon.CodecRaw,
				rawtopiccommon.CodecGzip,
				customCodecSupported,
			},
			expectedResult: rawtopiccommon.SupportedCodecs{
				customCodecSupported,
			},
		},
		{
			name:  "ForceRawWithDeniedByServer",
			force: rawtopiccommon.CodecRaw,
			serverCodecs: rawtopiccommon.SupportedCodecs{
				rawtopiccommon.CodecGzip,
			},
			expectedResult: nil,
		},
		{
			name:         "NotForcedWithEmptyServerList",
			force:        rawtopiccommon.CodecUNSPECIFIED,
			serverCodecs: nil,
			expectedResult: rawtopiccommon.SupportedCodecs{
				rawtopiccommon.CodecRaw,
				rawtopiccommon.CodecGzip,
			},
		},
		{
			name:  "NotForcedWithServerGzipOnly",
			force: rawtopiccommon.CodecUNSPECIFIED,
			serverCodecs: rawtopiccommon.SupportedCodecs{
				rawtopiccommon.CodecGzip,
			},
			expectedResult: rawtopiccommon.SupportedCodecs{
				rawtopiccommon.CodecGzip,
			},
		},
		{
			name:  "NotForcedCustomCodecSupportedAndAllowedByServer",
			force: rawtopiccommon.CodecUNSPECIFIED,
			serverCodecs: rawtopiccommon.SupportedCodecs{
				rawtopiccommon.CodecGzip,
				customCodecSupported,
				customCodecUnsupported,
			},
			expectedResult: rawtopiccommon.SupportedCodecs{
				rawtopiccommon.CodecGzip,
				customCodecSupported,
			},
		},
	}

	for _, test := range table {
		t.Run(test.name, func(t *testing.T) {
			res := calculateAllowedCodecs(test.force, encoders, test.serverCodecs)
			require.Equal(t, test.expectedResult, res)
		})
	}
}

func newTestMessageWithDataContent(num int) messageWithDataContent {
	res := newMessageDataWithContent(PublicMessage{SeqNo: int64(num)}, testCommonEncoders)
	return res
}

func newTestMessages(numbers ...int) []PublicMessage {
	messages := make([]PublicMessage, len(numbers))
	for i, num := range numbers {
		messages[i].SeqNo = int64(num)
	}
	return messages
}

func newTestMessagesWithContent(numbers ...int) []messageWithDataContent {
	messages := make([]messageWithDataContent, 0, len(numbers))
	for _, num := range numbers {
		messages = append(messages, newTestMessageWithDataContent(num))
	}
	return messages
}

func newTestWriterStopped(opts ...PublicWriterOption) *WriterReconnector {
	cfgOptions := append(defaultTestWriterOptions(), opts...)
	cfg := newWriterReconnectorConfig(cfgOptions...)
	res := newWriterReconnectorStopped(cfg)

	if cfg.AdditionalEncoders == nil {
		res.encodersMap = testCommonEncoders
	}

	return res
}

func defaultTestWriterOptions() []PublicWriterOption {
	return []PublicWriterOption{
		WithProducerID("test-producer-id"),
		WithTopic("test-topic"),
		WithSessionMeta(map[string]string{"test-key": "test-val"}),
		WithPartitioning(NewPartitioningWithMessageGroupID("test-message-group-id")),
		WithAutoSetSeqNo(false),
		WithWaitAckOnWrite(false),
		WithCodec(rawtopiccommon.CodecRaw),
		WithAutosetCreatedTime(false),
	}
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case _, existVal := <-ch:
		if existVal {
			panic("value, when not expected")
		}
		return true
	default:
		return false
	}
}

type testEnv struct {
	ctx                   context.Context
	stream                *MockRawTopicWriterStream
	writer                *WriterReconnector
	sendFromServerChannel chan sendFromServerResponse
	stopReadEvents        empty.Chan
	partitionID           int64
	connectCount          int64
}

type testEnvOptions struct {
	writerOptions []PublicWriterOption
	lastSeqNo     int64
	topicCodecs   rawtopiccommon.SupportedCodecs
}

func newTestEnv(t testing.TB, options *testEnvOptions) *testEnv {
	if options == nil {
		options = &testEnvOptions{}
	}

	res := &testEnv{
		ctx:                   xtest.Context(t),
		stream:                NewMockRawTopicWriterStream(gomock.NewController(t)),
		sendFromServerChannel: make(chan sendFromServerResponse, 1),
		stopReadEvents:        make(empty.Chan),
		partitionID:           14,
	}

	writerOptions := append(defaultTestWriterOptions(), WithConnectFunc(func(ctx context.Context) (
		RawTopicWriterStream,
		error,
	) {
		connectNum := atomic.AddInt64(&res.connectCount, 1)
		if connectNum > 1 {
			t.Fatalf("test: default env support most one connection")
		}
		return res.stream, nil
	}))
	writerOptions = append(writerOptions, options.writerOptions...)

	res.writer = newWriterReconnectorStopped(newWriterReconnectorConfig(writerOptions...))

	res.stream.EXPECT().Recv().DoAndReturn(res.receiveMessageHandler).AnyTimes()

	req := testCreateInitRequest(res.writer)

	res.stream.EXPECT().Send(&req).Do(func(_ interface{}) {
		supportedCodecs := rawtopiccommon.SupportedCodecs{rawtopiccommon.CodecRaw}
		if options.topicCodecs != nil {
			supportedCodecs = options.topicCodecs
		}
		res.sendFromServer(&rawtopicwriter.InitResult{
			ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{},
			LastSeqNo:             options.lastSeqNo,
			SessionID:             "session-" + t.Name(),
			PartitionID:           res.partitionID,
			SupportedCodecs:       supportedCodecs,
		})
	}).Return(nil)

	streamClosed := make(empty.Chan)
	res.stream.EXPECT().CloseSend().Do(func() {
		close(streamClosed)
	})

	res.writer.start()
	require.NoError(t, res.writer.waitFirstInitResponse(res.ctx))

	t.Cleanup(func() {
		res.writer.close(context.Background(), errors.New("stop writer test environment"))
		close(res.stopReadEvents)
		<-streamClosed
	})
	return res
}

func (e *testEnv) sendFromServer(msg rawtopicwriter.ServerMessage) {
	if msg.StatusData().Status == 0 {
		msg.SetStatus(rawydb.StatusSuccess)
	}

	e.sendFromServerChannel <- sendFromServerResponse{msg: msg}
}

func (e *testEnv) receiveMessageHandler() (rawtopicwriter.ServerMessage, error) {
	select {
	case <-e.stopReadEvents:
		return nil, fmt.Errorf("test: stop test environment")
	case res := <-e.sendFromServerChannel:
		return res.msg, res.err
	}
}

type sendFromServerResponse struct {
	msg rawtopicwriter.ServerMessage
	err error
}

func testCreateInitRequest(w *WriterReconnector) rawtopicwriter.InitRequest {
	req := newSingleStreamWriterStopped(context.Background(), w.createWriterStreamConfig(nil)).createInitRequest()
	return req
}
