package topiclistenerinternal

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestPartitionWorkerDebitsLocalBufferBeforeReportingInvalidBatch(t *testing.T) {
	ctx := xtest.Context(t)
	sender := newSyncMessageSender()
	batch := createTestBatchWithBufferBytes(t, 42)
	var deltas []int
	var deltasAtValidationError []int
	tracer := &trace.Topic{
		OnPartitionWorkerProcessMessage: func(trace.TopicPartitionWorkerProcessMessageStartInfo) func(
			trace.TopicPartitionWorkerProcessMessageDoneInfo,
		) {
			return func(info trace.TopicPartitionWorkerProcessMessageDoneInfo) {
				require.Error(t, info.Error)
				deltasAtValidationError = append(deltasAtValidationError, deltas...)
				require.Empty(t, sender.GetFreedBuffer())
			}
		},
	}
	worker := NewPartitionWorker(
		123,
		topicreadercommon.BatchGetPartitionSession(batch),
		sender,
		NewMockEventHandler(gomock.NewController(t)),
		func(rawtopicreader.PartitionSessionID, error) {},
		tracer,
		"test-listener",
	)
	defer worker.messageQueue.Close()
	worker.onLocalBufferDelta = func(delta int) bool {
		deltas = append(deltas, delta)

		return delta > 0
	}
	require.True(t, worker.AddMessagesBatch(
		rawtopiccommon.ServerMessageMetadata{Status: rawydb.StatusInternalError}, batch,
	))
	queued, ok, err := worker.messageQueue.Receive(ctx)
	require.NoError(t, err)
	require.True(t, ok)

	require.Error(t, worker.processUnifiedMessage(ctx, queued))
	require.Equal(t, []int{1, -1}, deltasAtValidationError)
	require.Equal(t, []int{1, -1}, deltas)
	require.Equal(t, []int{42}, sender.GetFreedBuffer())
}
