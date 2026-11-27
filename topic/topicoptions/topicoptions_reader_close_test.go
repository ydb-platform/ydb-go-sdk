package topicoptions_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreaderinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestReaderSessionErrorPanicPreservesInitError(t *testing.T) {
	connectErr := status.Error(codes.PermissionDenied, "access denied")
	called := make(chan struct{})
	internalReader, err := topicreaderinternal.NewReader(nil,
		func(context.Context, int64, *trace.Topic) (topicreadercommon.RawTopicReaderStream, error) {
			return nil, connectErr
		}, "consumer", []topicreadercommon.PublicReadSelector{{Path: "/topic"}},
		topicoptions.WithReaderTrace(trace.Topic{
			OnReaderSessionError: func(trace.TopicReaderSessionErrorInfo) {
				close(called)
				panic("session error observer")
			},
		}),
	)
	require.NoError(t, err)
	reader := topicreader.NewReader(internalReader)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case <-called:
	case <-ctx.Done():
		t.Fatal("session-error callback did not run")
	}
	require.ErrorIs(t, reader.WaitInit(ctx), connectErr)
	require.NoError(t, reader.Close(ctx))
}

func TestReaderCloseFromWorkerTrace(t *testing.T) {
	for _, hook := range []string{"first-credit", "messages-received"} {
		t.Run(hook, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			ready := make(chan struct{})
			result := make(chan error, 1)
			var reader *topicreader.Reader
			closeReader := func() {
				<-ready
				err := reader.Close(ctx)
				defer func() { result <- err }()
				if err == nil {
					panic("observer panic after worker was joined")
				}
			}
			var tracer trace.Topic
			stream := &readerCloseStream{messages: make(chan rawtopicreader.ServerMessage, 3), closed: make(chan struct{})}
			stream.messages <- &rawtopicreader.InitResponse{SessionID: "session"}
			if hook == "first-credit" {
				tracer.OnReaderCreditBalanceChanged = func(info trace.TopicReaderCreditBalanceChangedInfo) {
					if info.BytesDelta > 0 {
						closeReader()
					}
				}
			} else {
				tracer.OnReaderMessagesReceived = func(trace.TopicReaderMessagesReceivedInfo) { closeReader() }
				stream.messages <- &rawtopicreader.StartPartitionSessionRequest{
					PartitionSession: rawtopicreader.PartitionSession{PartitionSessionID: 1, Path: "/topic"},
				}
				stream.messages <- &rawtopicreader.ReadResponse{
					PartitionData: []rawtopicreader.PartitionData{{
						PartitionSessionID: 1,
						Batches:            []rawtopicreader.Batch{{MessageData: []rawtopicreader.MessageData{{}}}},
					}},
				}
			}
			internalReader, err := topicreaderinternal.NewReader(nil,
				func(context.Context, int64, *trace.Topic) (topicreadercommon.RawTopicReaderStream, error) {
					return stream, nil
				}, "consumer", []topicreadercommon.PublicReadSelector{{Path: "/topic"}},
				topicoptions.WithReaderStartTimeout(time.Minute),
				topicoptions.WithReaderTrace(tracer),
			)
			require.NoError(t, err)
			reader = topicreader.NewReader(internalReader)
			close(ready)
			select {
			case err := <-result:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal("worker trace did not return from Reader.Close")
			}
		})
	}
}

func TestReaderCloseFromTerminalSessionError(t *testing.T) {
	var reader *topicreader.Reader
	var stops atomic.Int32
	ready := make(chan struct{})
	callbackReturned := make(chan error, 1)
	connectErr := status.Error(codes.PermissionDenied, "access denied")
	internalReader, err := topicreaderinternal.NewReader(nil,
		func(context.Context, int64, *trace.Topic) (topicreadercommon.RawTopicReaderStream, error) {
			<-ready

			return nil, connectErr
		}, "consumer", []topicreadercommon.PublicReadSelector{{Path: "/topic"}},
		topicoptions.WithReaderTrace(trace.Topic{
			OnReaderSessionError: func(info trace.TopicReaderSessionErrorInfo) {
				if info.RetryDecision == "stop" {
					stops.Add(1)
				}
				callbackReturned <- reader.Close(context.Background())
			},
		}),
	)
	require.NoError(t, err)
	reader = topicreader.NewReader(internalReader)
	close(ready)

	select {
	case err := <-callbackReturned:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("session-error callback did not return from Reader.Close")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.ErrorIs(t, reader.WaitInit(ctx), connectErr)
	require.NoError(t, reader.Close(ctx))
	require.EqualValues(t, 1, stops.Load())
}

func TestReaderCloseFromRetrySessionError(t *testing.T) {
	var reader *topicreader.Reader
	ready := make(chan struct{})
	callbackStarted := make(chan struct{}, 1)
	callbackReturned := make(chan error, 1)
	connectErr := xerrors.Transport(status.Error(codes.Unavailable, "connection unavailable"))
	internalReader, err := topicreaderinternal.NewReader(nil,
		func(context.Context, int64, *trace.Topic) (topicreadercommon.RawTopicReaderStream, error) {
			<-ready

			return nil, connectErr
		}, "consumer", []topicreadercommon.PublicReadSelector{{Path: "/topic"}},
		topicoptions.WithReaderStartTimeout(time.Minute),
		topicoptions.WithReaderTrace(trace.Topic{
			OnReaderSessionError: func(info trace.TopicReaderSessionErrorInfo) {
				if info.RetryDecision == "retry" {
					callbackStarted <- struct{}{}
					callbackReturned <- reader.Close(context.Background())
				}
			},
		}),
	)
	require.NoError(t, err)
	reader = topicreader.NewReader(internalReader)
	close(ready)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case <-callbackStarted:
	case <-ctx.Done():
		t.Fatal("retry session-error callback did not start")
	}
	select {
	case err := <-callbackReturned:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("retry session-error callback did not return from Reader.Close")
	}
	require.ErrorContains(t, reader.WaitInit(ctx), "ydb: reader closed")
	require.NoError(t, reader.Close(ctx))
}

type readerCloseStream struct {
	messages chan rawtopicreader.ServerMessage
	closed   chan struct{}
}

func (s *readerCloseStream) Recv() (rawtopicreader.ServerMessage, error) {
	select {
	case msg := <-s.messages:
		return msg, nil
	case <-s.closed:
		return nil, context.Canceled
	}
}

func (*readerCloseStream) Send(rawtopicreader.ClientMessage) error { return nil }

func (s *readerCloseStream) CloseSend() error {
	close(s.closed)

	return nil
}
