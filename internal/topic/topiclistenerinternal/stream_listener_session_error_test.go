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

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTopicListenerSessionErrorUsesRetryDecisionOnce(t *testing.T) {
	for _, test := range []struct {
		name     string
		decision topic.PublicCheckRetryResult
		expected string
	}{
		{name: "retry", decision: topic.PublicRetryDecisionRetry, expected: "retry"},
		{name: "stop", decision: topic.PublicRetryDecisionStop, expected: "stop"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var events []trace.TopicReaderSessionErrorInfo
			checkCalls := 0
			cfg := NewStreamListenerConfig()
			cfg.ReaderInfo = topicreadercommon.ReaderInfo{
				Endpoint:   "endpoint",
				Database:   "/database",
				Consumer:   "consumer",
				ReaderName: "reader",
				Listener:   true,
			}
			cfg.Tracer = &trace.Topic{
				OnReaderSessionError: func(info trace.TopicReaderSessionErrorInfo) {
					events = append(events, info)
				},
			}
			cfg.CheckError = func(topic.PublicCheckErrorRetryArgs) topic.PublicCheckRetryResult {
				checkCalls++

				return test.decision
			}
			reason := grpcStatus.Error(grpcCodes.Unavailable, "connection lost")
			listener := &TopicListenerReconnector{
				streamConfig: &cfg,
				client:       freshStreamTopicClient{},
			}
			if test.expected == "retry" {
				setListenerRetryBackoff(&cfg, listenerTestBackoff{})
			}

			stream, result := listener.retryConnect(context.Background(), reason)
			if test.expected == "retry" {
				require.NoError(t, result)
				require.NotNil(t, stream)
			} else {
				require.ErrorIs(t, result, reason)
			}
			require.Equal(t, 1, checkCalls)
			require.Len(t, events, 1)
			require.Equal(t, "endpoint", events[0].Endpoint)
			require.Equal(t, "/database", events[0].Database)
			require.Equal(t, "consumer", events[0].Consumer)
			require.Equal(t, "reader", events[0].ReaderName)
			require.Equal(t, test.expected, events[0].RetryDecision)
			require.Equal(t, "Unavailable", events[0].StatusCode)
			require.Equal(t, "transport_error", events[0].ErrorType)
			if stream != nil {
				require.NoError(t, stream.Close(context.Background(), ErrUserCloseTopic))
			}
		})
	}
}

func TestTopicListenerSessionErrorSuppressesExpectedTermination(t *testing.T) {
	var events []trace.TopicReaderSessionErrorInfo
	cfg := NewStreamListenerConfig()
	cfg.Tracer = &trace.Topic{
		OnReaderSessionError: func(info trace.TopicReaderSessionErrorInfo) {
			events = append(events, info)
		},
	}
	listener := &TopicListenerReconnector{streamConfig: &cfg}

	_, err := listener.retryConnect(context.Background(), ErrUserCloseTopic)
	require.ErrorIs(t, err, ErrUserCloseTopic)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = listener.retryConnect(ctx, context.Canceled)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, events)
}

func TestTopicListenerSessionErrorKeepsDeadlineFailures(t *testing.T) {
	var events []trace.TopicReaderSessionErrorInfo
	cfg := NewStreamListenerConfig()
	cfg.Tracer = &trace.Topic{
		OnReaderSessionError: func(info trace.TopicReaderSessionErrorInfo) {
			events = append(events, info)
		},
	}
	cfg.CheckError = func(topic.PublicCheckErrorRetryArgs) topic.PublicCheckRetryResult {
		return topic.PublicRetryDecisionStop
	}
	listener := &TopicListenerReconnector{streamConfig: &cfg}

	_, err := listener.retryConnect(
		context.Background(),
		grpcStatus.Error(grpcCodes.DeadlineExceeded, "connect timeout"),
	)
	require.Error(t, err)
	require.Len(t, events, 1)
	require.Equal(t, "DeadlineExceeded", events[0].StatusCode)
	require.Equal(t, "transport_error", events[0].ErrorType)
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
