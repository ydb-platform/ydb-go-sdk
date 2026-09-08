package topicreadercommon

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestAppendCommitMessageMetadataHandlesEmptySides(t *testing.T) {
	lhs := []commitMessageMetadata{{offset: 10}}
	rhs := []commitMessageMetadata{{offset: 14}}

	require.Equal(t, rhs, appendCommitMessageMetadata(nil, rhs, 0, len(rhs)))
	require.Equal(t, lhs, appendCommitMessageMetadata(lhs, nil, len(lhs), 0))
}

func TestCommitMessageMetadataForMessageFallbacks(t *testing.T) {
	session := NewPartitionSession(context.Background(), "topic", 1, 1, "", 2, 3, 0)
	t.Cleanup(session.Close)

	// A message created before metrics are enabled has only its legacy
	// contiguous range. NewBatch can still derive its single logical offset.
	message := NewPublicMessageBuilder().
		PartitionSession(session).
		Offset(20).
		Build()
	session.SetupCommitMetrics(&trace.Topic{
		OnReaderCommitQueued: func(trace.TopicReaderCommitQueuedInfo) {},
	}, ReaderInfo{})

	batch, err := NewBatch(session, []*PublicMessage{message})
	require.NoError(t, err)
	require.Equal(t, []rawtopiccommon.Offset{20}, GetCommitRange(batch).MessageOffsets())

	legacyRangeMessage := NewPublicMessageBuilder().
		CommitRange(CommitRange{
			CommitOffsetStart: 22,
			CommitOffsetEnd:   24,
			PartitionSession:  session,
		}).
		Build()
	batch, err = NewBatch(session, []*PublicMessage{legacyRangeMessage})
	require.NoError(t, err)
	require.Nil(t, GetCommitRange(batch).MessageOffsets())

	// A range containing several captured messages is not a valid identity for
	// one PublicMessage, so the resulting batch deliberately has no metadata.
	source, err := NewBatchFromStream(
		NewMultiDecoder(),
		session,
		rawtopicreader.Batch{
			Codec: rawtopiccommon.CodecRaw,
			MessageData: []rawtopicreader.MessageData{
				{Offset: 30},
				{Offset: 34},
			},
		},
	)
	require.NoError(t, err)
	require.Equal(t, []rawtopiccommon.Offset{30, 34}, GetCommitRange(source).MessageOffsets())
	malformedSingle := NewPublicMessageBuilder().
		CommitRange(GetCommitRange(source)).
		Build()

	batch, err = NewBatch(session, []*PublicMessage{malformedSingle})
	require.NoError(t, err)
	require.Nil(t, GetCommitRange(batch).MessageOffsets())
}

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

func TestCommitRangeSnapshotPublicConstructorsClipMetadata(t *testing.T) {
	session := NewPartitionSession(context.Background(), "topic", 1, 1, "", 2, 3, 0)
	t.Cleanup(session.Close)
	metadata := make([]commitMessageMetadata, 1, 4)
	metadata[0].offset = 41
	source := CommitRange{
		CommitOffsetStart: 41,
		CommitOffsetEnd:   42,
		PartitionSession:  session,
		messageMetadata:   metadata,
	}

	public := source.getCommitRange()
	require.Equal(t, len(public.priv.messageMetadata), cap(public.priv.messageMetadata))

	got := GetCommitRange(public)
	require.Equal(t, []rawtopiccommon.Offset{41}, got.MessageOffsets())
	require.Equal(t, len(got.messageMetadata), cap(got.messageMetadata))

	ranges := NewCommitRangesFromPublicCommits([]PublicCommitRange{public})
	require.Len(t, ranges.Ranges, 1)
	require.Equal(t, []rawtopiccommon.Offset{41}, ranges.Ranges[0].MessageOffsets())
	require.Equal(t, len(ranges.Ranges[0].messageMetadata), cap(ranges.Ranges[0].messageMetadata))
}

func TestPublicMessageBuilderCommitRangeAddsSingleMetadata(t *testing.T) {
	session := newCommitMetricsTestSession(t, &trace.Topic{
		OnReaderCommitQueued: func(trace.TopicReaderCommitQueuedInfo) {},
	})

	message := NewPublicMessageBuilder().
		CommitRange(CommitRange{
			CommitOffsetStart: 51,
			CommitOffsetEnd:   52,
			PartitionSession:  session,
		}).
		Build()

	require.Equal(t, []rawtopiccommon.Offset{51}, GetCommitRange(message).MessageOffsets())
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
