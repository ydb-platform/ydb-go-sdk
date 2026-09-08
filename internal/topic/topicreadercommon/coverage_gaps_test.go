package topicreadercommon

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestSetupCommitMetricsGuardsAndRepeatedInitialization(t *testing.T) {
	var nilSession *PartitionSession
	require.NotPanics(t, func() {
		nilSession.SetupCommitMetrics(&trace.Topic{}, ReaderInfo{})
	})

	session := NewPartitionSession(context.Background(), "topic", 1, 1, "", 2, 3, 0)
	t.Cleanup(session.Close)
	require.NotPanics(t, func() {
		session.SetupCommitMetrics(nil, ReaderInfo{})
	})
	require.Nil(t, session.commitMetrics)

	session.SetupCommitMetrics(&trace.Topic{}, ReaderInfo{})
	require.Nil(t, session.commitMetrics)

	cancelledContext, cancel := context.WithCancel(context.Background())
	cancel()
	cancelledSession := NewPartitionSession(cancelledContext, "topic", 1, 1, "", 2, 3, 0)
	t.Cleanup(cancelledSession.Close)
	cancelledSession.SetupCommitMetrics(&trace.Topic{
		OnReaderCommitQueued: func(trace.TopicReaderCommitQueuedInfo) {},
	}, ReaderInfo{})
	require.Nil(t, cancelledSession.commitMetrics)

	queuedTracer := &trace.Topic{
		OnReaderCommitQueued: func(trace.TopicReaderCommitQueuedInfo) {},
	}
	session.SetupCommitMetrics(queuedTracer, ReaderInfo{})
	require.NotNil(t, session.commitMetrics)
	initialMetrics := session.commitMetrics

	session.SetupCommitMetrics(&trace.Topic{
		OnReaderCommitAcknowledged: func(trace.TopicReaderCommitAcknowledgedInfo) {},
	}, ReaderInfo{})
	require.Same(t, initialMetrics, session.commitMetrics)
	require.Nil(t, session.commitMetrics.tracker)
}

func TestTraceCommitAcknowledgedAfterRegistrationWithoutHook(t *testing.T) {
	session := newCommitMetricsTestSession(t, &trace.Topic{
		OnReaderCommitQueued: func(trace.TopicReaderCommitQueuedInfo) {},
	})

	require.NotPanics(t, func() {
		TraceCommitAcknowledgedAfterRegistration(context.Background(), session, 1)
	})
}

func TestTraceReaderSessionErrorHandlesNoOpAndUnknownTransportCode(t *testing.T) {
	readerInfo := ReaderInfo{
		Endpoint:   "endpoint",
		Database:   "database",
		Consumer:   "consumer",
		ReaderName: "reader",
	}
	unknownTransportError := status.Error(codes.Code(99), "future transport code")

	TraceReaderSessionError(context.Background(), nil, readerInfo, "stop", unknownTransportError)
	TraceReaderSessionError(context.Background(), &trace.Topic{}, readerInfo, "stop", unknownTransportError)

	calls := 0
	tracer := &trace.Topic{
		OnReaderSessionError: func(trace.TopicReaderSessionErrorInfo) {
			calls++
		},
	}
	TraceReaderSessionError(context.Background(), tracer, readerInfo, "stop", nil)
	require.Zero(t, calls)

	var actual trace.TopicReaderSessionErrorInfo
	tracer.OnReaderSessionError = func(info trace.TopicReaderSessionErrorInfo) {
		actual = info
	}
	TraceReaderSessionError(context.Background(), tracer, readerInfo, "stop", unknownTransportError)

	require.Equal(t, "stop", actual.RetryDecision)
	require.Equal(t, "unknown", actual.StatusCode)
	require.Equal(t, "transport_error", actual.ErrorType)
	require.Error(t, actual.Error)
	require.Equal(t, SessionErrorClassification{
		StatusCode: "unknown",
		ErrorType:  "transport_error",
	}, ClassifySessionError(unknownTransportError))
	require.ErrorIs(t, actual.Error, unknownTransportError)
}
