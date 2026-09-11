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
	var events []trace.TopicReaderSessionErrorInfo
	listener := newSessionErrorTestListener(&events)

	listener.goClose(context.Background(), grpcStatus.Error(grpcCodes.Unavailable, "connection lost"))
	listener.goClose(context.Background(), errors.New("second failure"))

	require.Len(t, events, 1)
	event := events[0]
	require.Equal(t, "endpoint", event.Endpoint)
	require.Equal(t, "/database", event.Database)
	require.Equal(t, "consumer", event.Consumer)
	require.Equal(t, "reader", event.ReaderName)
	require.Equal(t, "stop", event.RetryDecision)
	require.Equal(t, "Unavailable", event.StatusCode)
	require.Equal(t, "transport_error", event.ErrorType)

	_ = listener.background.Close(context.Background(), errors.New("test finished"))
}

func TestStreamListenerSessionErrorSkipsExpectedTermination(t *testing.T) {
	var events []trace.TopicReaderSessionErrorInfo
	listener := newSessionErrorTestListener(&events)
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	listener.traceSessionStop(context.Background(), ErrUserCloseTopic)
	listener.traceSessionStop(cancelledCtx, context.Canceled)
	listener.traceSessionStop(context.Background(), errPartitionQueueClosed)

	require.Empty(t, events)

	_ = listener.background.Close(context.Background(), errors.New("test finished"))
}

func TestStreamListenerSessionErrorKeepsDeadlineFailures(t *testing.T) {
	var events []trace.TopicReaderSessionErrorInfo
	listener := newSessionErrorTestListener(&events)

	listener.traceSessionStop(context.Background(), grpcStatus.Error(grpcCodes.DeadlineExceeded, "connect timeout"))

	require.Len(t, events, 1)
	require.Equal(t, "DeadlineExceeded", events[0].StatusCode)
	require.Equal(t, "transport_error", events[0].ErrorType)

	_ = listener.background.Close(context.Background(), errors.New("test finished"))
}

func TestTopicListenerReconnectorSessionErrorReportsInitialFailure(t *testing.T) {
	var events []trace.TopicReaderSessionErrorInfo
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
			events = append(events, info)
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
	require.Len(t, events, 1)
	require.Equal(t, "stop", events[0].RetryDecision)
	require.Equal(t, "UNAUTHORIZED", events[0].StatusCode)
	require.Equal(t, "ydb_error", events[0].ErrorType)

	require.NoError(t, reconnector.Close(context.Background(), errors.New("test finished")))
}

func newSessionErrorTestListener(events *[]trace.TopicReaderSessionErrorInfo) *streamListener {
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
				*events = append(*events, info)
			},
		},
	}
	listener.streamClose = func(error) {}

	return listener
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
