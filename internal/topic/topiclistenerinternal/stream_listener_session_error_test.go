package topiclistenerinternal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestStreamListenerSessionErrorReportsActualStopOnce(t *testing.T) {
	events := make(chan trace.TopicReaderSessionErrorInfo, 4)
	listener := newSessionErrorTestListener(events)

	listener.goClose(context.Background(), grpcStatus.Error(grpcCodes.Unavailable, "connection lost"))
	listener.goClose(context.Background(), errors.New("second failure"))

	select {
	case event := <-events:
		require.Equal(t, "endpoint", event.Endpoint)
		require.Equal(t, "/database", event.Database)
		require.Equal(t, "consumer", event.Consumer)
		require.Equal(t, "reader", event.ReaderName)
		require.Equal(t, "stop", event.RetryDecision)
		require.Equal(t, "Unavailable", event.StatusCode)
		require.Equal(t, "transport_error", event.ErrorType)
	case <-time.After(time.Second):
		t.Fatal("session error event was not emitted")
	}

	select {
	case event := <-events:
		t.Fatalf("unexpected duplicate session error event: %+v", event)
	case <-time.After(20 * time.Millisecond):
	}

	_ = listener.background.Close(context.Background(), errors.New("test finished"))
}

func TestStreamListenerSessionErrorSkipsExpectedTermination(t *testing.T) {
	events := make(chan trace.TopicReaderSessionErrorInfo, 4)
	listener := newSessionErrorTestListener(events)
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	listener.traceSessionStop(context.Background(), ErrUserCloseTopic)
	listener.traceSessionStop(cancelledCtx, context.Canceled)
	listener.traceSessionStop(context.Background(), errPartitionQueueClosed)

	select {
	case event := <-events:
		t.Fatalf("unexpected session error event: %+v", event)
	case <-time.After(20 * time.Millisecond):
	}

	_ = listener.background.Close(context.Background(), errors.New("test finished"))
}

func TestStreamListenerSessionErrorKeepsDeadlineFailures(t *testing.T) {
	events := make(chan trace.TopicReaderSessionErrorInfo, 4)
	listener := newSessionErrorTestListener(events)

	listener.traceSessionStop(context.Background(), grpcStatus.Error(grpcCodes.DeadlineExceeded, "connect timeout"))

	select {
	case event := <-events:
		require.Equal(t, "DeadlineExceeded", event.StatusCode)
		require.Equal(t, "transport_error", event.ErrorType)
	case <-time.After(time.Second):
		t.Fatal("deadline session error event was not emitted")
	}

	_ = listener.background.Close(context.Background(), errors.New("test finished"))
}

func TestTopicListenerReconnectorSessionErrorReportsInitialFailure(t *testing.T) {
	events := make(chan trace.TopicReaderSessionErrorInfo, 4)
	connectErr := xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAUTHORIZED))
	cfg := NewStreamListenerConfig()
	cfg.Consumer = "consumer"
	cfg.Selectors = []*topicreadercommon.PublicReadSelector{{Path: "topic"}}
	cfg.ReaderInfo = topicreadercommon.ReaderInfo{
		Endpoint:   "endpoint",
		Database:   "/database",
		Consumer:   "consumer",
		ReaderName: "reader",
	}
	cfg.Tracer = &trace.Topic{
		OnReaderSessionError: func(info trace.TopicReaderSessionErrorInfo) {
			reportSessionErrorEvent(events, info)
		},
	}

	reconnector, err := NewTopicListenerReconnector(
		&failingTopicClient{err: connectErr},
		&cfg,
		nil,
	)
	require.NoError(t, err)

	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.ErrorIs(t, reconnector.WaitInit(waitCtx), connectErr)
	select {
	case event := <-events:
		require.Equal(t, "stop", event.RetryDecision)
		require.Equal(t, "UNAUTHORIZED", event.StatusCode)
		require.Equal(t, "ydb_error", event.ErrorType)
	case <-time.After(time.Second):
		t.Fatal("listener initial session error event was not emitted")
	}

	require.NoError(t, reconnector.Close(context.Background(), errors.New("test finished")))
}

func newSessionErrorTestListener(events chan<- trace.TopicReaderSessionErrorInfo) *streamListener {
	ctx := context.Background()
	listener := &streamListener{
		cfg: &StreamListenerConfig{
			ReaderInfo: topicreadercommon.ReaderInfo{
				Endpoint:   "endpoint",
				Database:   "/database",
				Consumer:   "consumer",
				ReaderName: "reader",
			},
		},
		background: *background.NewWorker(ctx, "session-error-test"),
		sessions:   &topicreadercommon.PartitionSessionStorage{},
		tracer: &trace.Topic{
			OnReaderSessionError: func(info trace.TopicReaderSessionErrorInfo) {
				reportSessionErrorEvent(events, info)
			},
		},
	}
	listener.streamClose = func(error) {}

	return listener
}

func reportSessionErrorEvent(
	events chan<- trace.TopicReaderSessionErrorInfo,
	info trace.TopicReaderSessionErrorInfo,
) {
	select {
	case events <- info:
	default:
	}
}

type failingTopicClient struct {
	err error
}

func (c *failingTopicClient) StreamRead(
	context.Context,
	int64,
	*trace.Topic,
) (rawtopicreader.StreamReader, error) {
	return rawtopicreader.StreamReader{}, c.err
}
