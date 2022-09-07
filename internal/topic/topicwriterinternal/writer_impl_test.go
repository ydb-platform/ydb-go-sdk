package topicwriterinternal

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

var testCommonEncoders = NewEncoderMap()

func TestWriterImpl_AutoSeq(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		ctx := xtest.Context(t)
		w := newWriterImplStopped(writerImplConfig{autoSetSeqNo: true})
		w.firstInitResponseProcessed.Store(true)

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

		w := newWriterImplStopped(writerImplConfig{autoSetSeqNo: true})
		w.firstInitResponseProcessed.Store(true)
		require.Error(t, w.Write(ctx, newTestMessages(1)))
	})
}

func TestWriterImpl_CreateInitMessage(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		cfg := writerImplConfig{
			producerID:          "producer",
			topic:               "topic",
			writerMeta:          map[string]string{"key": "val"},
			defaultPartitioning: rawtopicwriter.NewPartitioningPartitionID(5),
			autoSetSeqNo:        false,
		}
		w := newWriterImplStopped(cfg)
		expected := rawtopicwriter.InitRequest{
			Path:             w.cfg.topic,
			ProducerID:       w.cfg.producerID,
			WriteSessionMeta: w.cfg.writerMeta,
			Partitioning:     w.cfg.defaultPartitioning,
			GetLastSeqNo:     w.cfg.autoSetSeqNo,
		}
		require.Equal(t, expected, w.createInitRequest())
	})

	t.Run("WithAutoSeq", func(t *testing.T) {
		t.Run("InitState", func(t *testing.T) {
			w := newWriterImplStopped(writerImplConfig{
				autoSetSeqNo: true,
			})
			require.True(t, w.createInitRequest().GetLastSeqNo)
		})
		t.Run("WithInternalSeqNo", func(t *testing.T) {
			w := newWriterImplStopped(writerImplConfig{
				autoSetSeqNo: true,
			})
			w.lastSeqNo = 1
			require.False(t, w.createInitRequest().GetLastSeqNo)
		})
	})
}

func TestWriterImpl_Write(t *testing.T) {
	t.Run("PushToQueue", func(t *testing.T) {
		ctx := context.Background()
		w := newTestWriterStopped()
		w.cfg.fillEmptyCreatedTime = false
		w.firstInitResponseProcessed.Store(true)

		err := w.Write(ctx, newTestMessages(1, 3, 5))
		require.NoError(t, err)

		expectedMap := map[int]messageWithDataContent{
			1: newTestMessageWithDataContent(1),
			2: newTestMessageWithDataContent(3),
			3: newTestMessageWithDataContent(5),
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
				err := e.writer.Write(e.ctx, []Message{{SeqNo: seqNo, CreatedAt: messageTime, Data: bytes.NewReader(messageData)}})
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
		require.NoError(t, e.writer.Write(e.ctx, []Message{{
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
		require.NoError(t, e.writer.Write(e.ctx, []Message{{
			Data: bytes.NewReader(messContent),
		}}))

		require.Equal(t, rawtopiccommon.CodecGzip, <-messReceived)
	})
	t.Run("Auto", func(t *testing.T) {
		var err error
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
		}).Times(2)

		codecs := make(map[rawtopiccommon.Codec]empty.Struct)

		require.NoError(t, err)
		require.NoError(t, e.writer.Write(e.ctx, []Message{{
			Data: bytes.NewReader(messContentShort),
		}}))
		// wait send
		codec := <-messReceived
		codecs[codec] = empty.Struct{}

		require.NoError(t, err)
		require.NoError(t, e.writer.Write(e.ctx, []Message{{
			Data: bytes.NewReader(messContentLong),
		}}))
		// wait send
		codec = <-messReceived
		codecs[codec] = empty.Struct{}

		// used two different codecs
		require.Len(t, codecs, 2)
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
	mc := gomock.NewController(t)
	strm := NewMockRawTopicWriterStream(mc)
	strm.EXPECT().Send(&rawtopicwriter.InitRequest{
		Path:             "test-topic",
		ProducerID:       "test-producer-id",
		WriteSessionMeta: map[string]string{"test-key": "test-val"},
		Partitioning: rawtopicwriter.Partitioning{
			Type:           rawtopicwriter.PartitioningMessageGroupID,
			MessageGroupID: "test-message-group-id",
		},
		GetLastSeqNo: true,
	})
	lastSeqNo := int64(123)
	strm.EXPECT().Recv().Return(&rawtopicwriter.InitResult{
		SessionID:       "test-session-id",
		SupportedCodecs: rawtopiccommon.SupportedCodecs{rawtopiccommon.CodecRaw, rawtopiccommon.CodecGzip},
		LastSeqNo:       lastSeqNo,
	}, nil)
	err := w.initStream(strm)
	require.NoError(t, err)
	require.Equal(t, "test-session-id", w.sessionID)
	require.Equal(t, lastSeqNo, w.lastSeqNo)
	require.True(t, isClosed(w.firstInitResponseProcessedChan))
}

func TestWriterImpl_Reconnect(t *testing.T) {
	t.Run("StartStopLoop", func(t *testing.T) {
		mc := gomock.NewController(t)
		strm := NewMockRawTopicWriterStream(mc)

		w := newTestWriterStopped()

		ctx := xtest.Context(t)
		ctx, cancel := xcontext.WithErrCancel(ctx)
		testErr := errors.New("test")

		connectCalled := false
		connectCalledChan := make(empty.Chan)
		var streamContext context.Context

		w.cfg.connect = func(streamCtxArg context.Context) (RawTopicWriterStream, error) {
			close(connectCalledChan)
			connectCalled = true
			streamContext = ctx
			require.NotEqual(t, ctx, streamCtxArg)
			streamContext = streamCtxArg
			return strm, nil
		}

		initRequest := w.createInitRequest()
		strm.EXPECT().Send(&initRequest)
		strm.EXPECT().Recv().Return(&rawtopicwriter.InitResult{
			ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{
				Status: rawydb.StatusSuccess,
			},
			LastSeqNo:       10,
			SessionID:       "test-session",
			PartitionID:     10,
			SupportedCodecs: rawtopiccommon.SupportedCodecs{rawtopiccommon.CodecRaw, rawtopiccommon.CodecGzip},
		}, nil).MaxTimes(2)
		strm.EXPECT().CloseSend().Do(func() {
			require.NoError(t, streamContext.Err())
			require.ErrorIs(t, ctx.Err(), testErr)
		})

		go func() {
			<-connectCalledChan
			cancel(testErr)
		}()

		w.sendLoop(ctx)
		require.True(t, connectCalled)
		require.Error(t, streamContext.Err())
	})

	t.Run("ReconnectOnErrors", func(t *testing.T) {
		ctx := xtest.Context(t)

		w := newTestWriterStopped()

		mc := gomock.NewController(t)

		type connectionAttemptContext struct {
			name            string
			stream          RawTopicWriterStream
			connectionError error
		}

		newStream := func(onSendInitCallback func()) *MockRawTopicWriterStream {
			strm := NewMockRawTopicWriterStream(mc)
			initReq := w.createInitRequest()

			streamClosed := make(empty.Chan)
			strm.EXPECT().CloseSend().Do(func() {
				close(streamClosed)
			})

			strm.EXPECT().Send(&initReq).Do(func(_ interface{}) {
				if onSendInitCallback != nil {
					onSendInitCallback()
				}
			})

			strm.EXPECT().Recv().Return(&rawtopicwriter.InitResult{
				ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{Status: rawydb.StatusSuccess},
			}, nil)

			strm.EXPECT().Recv().Do(func() {
				xtest.WaitChannelClosed(t, streamClosed)
			}).Return(nil, errors.New("test stream closed")).MaxTimes(1)
			return strm
		}

		strm2InitSent := make(empty.Chan)
		go func() {
			err := w.Write(ctx, newTestMessages(1))
			require.NoError(t, err)
		}()

		strm2 := newStream(func() {
			close(strm2InitSent)
		})
		strm2.EXPECT().Send(&rawtopicwriter.WriteRequest{
			Messages: []rawtopicwriter.MessageData{
				{SeqNo: 1},
			},
			Codec: rawtopiccommon.CodecRaw,
		}).Return(xerrors.Retryable(errors.New("retriable on strm2")))

		strm3 := newStream(nil)
		strm3.EXPECT().Send(&rawtopicwriter.WriteRequest{
			Messages: []rawtopicwriter.MessageData{
				{SeqNo: 1},
			},
			Codec: rawtopiccommon.CodecRaw,
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

		connectionAttempt := 0
		w.cfg.connect = func(ctx context.Context) (RawTopicWriterStream, error) {
			res := connectsResult[connectionAttempt]
			connectionAttempt++
			return res.stream, res.connectionError
		}

		w.sendLoop(ctx)
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

	// TODO: additional tests
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
				require.Equal(t, len(group), cap(group))
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

func TestWriterImpl_CalculateAllowedCodecs(t *testing.T) {
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
			name:           "ForceRawWithEmptyServer",
			force:          rawtopiccommon.CodecRaw,
			serverCodecs:   nil,
			expectedResult: rawtopiccommon.SupportedCodecs{rawtopiccommon.CodecRaw},
		},
		{
			name:           "ForceRawWithAllowedByServer",
			force:          rawtopiccommon.CodecRaw,
			serverCodecs:   rawtopiccommon.SupportedCodecs{rawtopiccommon.CodecRaw, rawtopiccommon.CodecGzip},
			expectedResult: rawtopiccommon.SupportedCodecs{rawtopiccommon.CodecRaw},
		},
		{
			name:           "ForceCustomWithAllowedByServer",
			force:          customCodecSupported,
			serverCodecs:   rawtopiccommon.SupportedCodecs{rawtopiccommon.CodecRaw, rawtopiccommon.CodecGzip, customCodecSupported},
			expectedResult: rawtopiccommon.SupportedCodecs{customCodecSupported},
		},
		{
			name:           "ForceRawWithDeniedByServer",
			force:          rawtopiccommon.CodecRaw,
			serverCodecs:   rawtopiccommon.SupportedCodecs{rawtopiccommon.CodecGzip},
			expectedResult: nil,
		},
		{
			name:           "NotForcedWithEmptyServerList",
			force:          rawtopiccommon.CodecUNSPECIFIED,
			serverCodecs:   nil,
			expectedResult: rawtopiccommon.SupportedCodecs{rawtopiccommon.CodecRaw, rawtopiccommon.CodecGzip},
		},
		{
			name:           "NotForcedWithServerGzipOnly",
			force:          rawtopiccommon.CodecUNSPECIFIED,
			serverCodecs:   rawtopiccommon.SupportedCodecs{rawtopiccommon.CodecGzip},
			expectedResult: rawtopiccommon.SupportedCodecs{rawtopiccommon.CodecGzip},
		},
		{
			name:           "NotForcedCustomCodecSupportedAndAllowedByServer",
			force:          rawtopiccommon.CodecUNSPECIFIED,
			serverCodecs:   rawtopiccommon.SupportedCodecs{rawtopiccommon.CodecGzip, customCodecSupported, customCodecUnsupported},
			expectedResult: rawtopiccommon.SupportedCodecs{rawtopiccommon.CodecGzip, customCodecSupported},
		},
	}

	for _, test := range table {
		t.Run(test.name, func(t *testing.T) {
			w := newWriterImplStopped(newWriterImplConfig())
			w.cfg.forceCodec = test.force
			w.encodersMap = encoders
			res := w.calculateAllowedCodecs(test.serverCodecs)
			require.Equal(t, test.expectedResult, res)
		})
	}
}

func newTestMessageWithDataContent(num int) messageWithDataContent {
	res, err := newMessageDataWithContent(Message{SeqNo: int64(num)}, testCommonEncoders, rawtopiccommon.CodecRaw)
	if err != nil {
		panic(err)
	}
	return res
}

func newTestMessages(numbers ...int) []Message {
	messages := make([]Message, 0, len(numbers))
	for _, num := range numbers {
		messages = append(messages, Message{SeqNo: int64(num)})
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

func newTestWriterStopped(opts ...PublicWriterOption) *WriterImpl {
	cfgOptions := append(defaultTestWriterOptions(), opts...)
	cfg := newWriterImplConfig(cfgOptions...)
	res := newWriterImplStopped(cfg)

	if cfg.additionalEncoders == nil {
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
	writer                *WriterImpl
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

	writerOptions := append(defaultTestWriterOptions(), WithConnectFunc(func(ctx context.Context) (RawTopicWriterStream, error) {
		connectNum := atomic.AddInt64(&res.connectCount, 1)
		if connectNum > 1 {
			t.Fatalf("test: default env support most one connection")
		}
		return res.stream, nil
	}))
	writerOptions = append(writerOptions, options.writerOptions...)

	res.writer = newWriterImplStopped(newWriterImplConfig(writerOptions...))

	res.stream.EXPECT().Recv().DoAndReturn(res.receiveMessageHandler).AnyTimes()

	req := res.writer.createInitRequest()
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
