package topicreaderinternal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestReaderReconnectorUsesReadStopDecision(t *testing.T) {
	streamErr := status.Error(codes.PermissionDenied, "access denied")
	e := newTopicReaderTestEnv(t)
	events := make(chan trace.TopicReaderSessionErrorInfo, 2)
	e.reader.cfg.Trace = sessionErrorTestTracer(events)
	reader := newMetricsReader(&e)
	r := reader.reader.(*readerReconnector)
	var policyCalls int
	r.retrySettings.CheckError = func(args topic.PublicCheckErrorRetryArgs) topic.PublicCheckRetryResult {
		policyCalls++
		if errors.Unwrap(args.Error) == nil {
			return topic.PublicRetryDecisionRetry
		}

		return topic.PublicRetryDecisionStop
	}
	e.Start()
	e.messagesFromServerToClient <- testStreamResult{err: streamErr}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case <-e.partitionSession.Context().Done():
	case <-ctx.Done():
		t.Fatal("partition session did not close after stream failure")
	}
	_, err := reader.ReadMessage(ctx)
	require.ErrorIs(t, err, streamErr)
	require.Empty(t, r.reconnectFromBadStream)
	require.Len(t, events, 1)
	event := <-events
	require.Equal(t, "stop", event.RetryDecision)
	require.Equal(t, "PermissionDenied", event.StatusCode)
	require.Equal(t, "transport_error", event.ErrorType)
	require.Equal(t, 1, policyCalls)
}
