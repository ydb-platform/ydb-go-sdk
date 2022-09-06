package topicwriterinternal

import (
	"context"
	"errors"
	"fmt"
	"testing"

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

func TestWriterImpl_AutoSeq(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
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
	ctx := context.Background()
	w := newTestWriter()

	w.firstInitResponseProcessed.Store(true)

	err := w.Write(ctx, newTestMessages(1, 3, 5))
	require.NoError(t, err)

	expectedMap := map[int]messageWithDataContent{
		1: newTestMessage(1),
		2: newTestMessage(3),
		3: newTestMessage(5),
	}

	require.Equal(t, expectedMap, w.queue.messagesByOrder)
}

func TestWriterImpl_InitSession(t *testing.T) {
	w := newTestWriter(WithAutoSetSeqNo(true))
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
	require.Equal(t, rawtopiccommon.SupportedCodecs{rawtopiccommon.CodecRaw, rawtopiccommon.CodecGzip}, w.allowedCodecsVal)
	require.Equal(t, lastSeqNo, w.lastSeqNo)
	require.True(t, isClosed(w.firstInitResponseProcessedChan))
}

func TestWriterImpl_Reconnect(t *testing.T) {
	t.Run("StartStopLoop", func(t *testing.T) {
		mc := gomock.NewController(t)
		strm := NewMockRawTopicWriterStream(mc)

		w := newTestWriter()

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
		}, nil)
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

		w := newTestWriter()

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
		require.True(t, allMessagesHasSameBufCodec(newTestMessages(1).m))
	})

	t.Run("SameCodecs", func(t *testing.T) {
		require.True(t, allMessagesHasSameBufCodec(newTestMessages(1, 2, 3).m))
	})
	t.Run("DifferCodecs", func(t *testing.T) {
		for i := 0; i < 3; i++ {
			messages := newTestMessages(1, 2, 3)
			messages.m[i].bufCodec = rawtopiccommon.CodecGzip
			require.False(t, allMessagesHasSameBufCodec(messages.m))
		}
	})
}

func TestCreateRawMessageData(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		req, err := createWriteRequest(nil)
		require.NoError(t, err)
		require.Equal(t,
			rawtopicwriter.WriteRequest{
				Messages: nil,
				Codec:    rawtopiccommon.CodecRaw,
			},
			req,
		)
	})

	t.Run("OK", func(t *testing.T) {
		messages := newTestMessages(2, 4, 10)
		messages.m[0].bufCodec = rawtopiccommon.CodecGzip
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
				mess := newTestMessage(index)
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

func newTestMessage(num int) messageWithDataContent {
	return messageWithDataContent{
		Message:  Message{SeqNo: int64(num)},
		buf:      newBuffer(),
		bufCodec: rawtopiccommon.CodecRaw,
	}
}

func newTestMessages(numbers ...int) *messageWithDataContentSlice {
	messages := newContentMessagesSlice()
	for _, num := range numbers {
		messages.m = append(messages.m, newTestMessage(num))
	}
	return messages
}

func newTestWriter(opts ...PublicWriterOption) WriterImpl {
	cfgOptions := []PublicWriterOption{
		WithProducerID("test-producer-id"),
		WithTopic("test-topic"),
		WithSessionMeta(map[string]string{"test-key": "test-val"}),
		WithPartitioning(NewPartitioningWithMessageGroupID("test-message-group-id")),
		WithAutoSetSeqNo(false),
	}
	cfgOptions = append(cfgOptions, opts...)
	cfg := newWriterImplConfig(cfgOptions...)
	return newWriterImplStopped(cfg)
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
