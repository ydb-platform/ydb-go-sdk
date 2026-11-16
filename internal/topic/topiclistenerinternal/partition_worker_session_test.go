package topiclistenerinternal

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestPartitionWorkerReadContextLivesUntilSessionStops(t *testing.T) {
	session := createTestPartitionSession()
	handler := NewMockEventHandler(gomock.NewController(t))
	var readContext context.Context
	handler.EXPECT().OnReadMessages(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, _ *PublicReadMessages) error {
			readContext = ctx

			return nil
		},
	)
	worker := NewPartitionWorker(session.StreamPartitionSessionID, session, newMockMessageSender(), handler,
		func(rawtopicreader.PartitionSessionID, error) {}, &trace.Topic{}, "test-listener")
	require.NoError(t, worker.processUnifiedMessage(xtest.Context(t), unifiedMessage{BatchMessage: &batchMessage{
		ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{Status: rawydb.StatusSuccess},
		Batch:                 createTestBatch(t, session),
	}}))
	require.NoError(t, readContext.Err(), "returning from the callback must not end the partition session")
	session.Close()
	xtest.WaitChannelClosed(t, readContext.Done())
}

func TestPartitionWorkerRevocationDoesNotHideUnrelatedHandlerErrors(t *testing.T) {
	for _, test := range []struct {
		name   string
		revoke bool
		err    error
	}{
		{name: "cancellation of a live session", err: context.Canceled},
		{name: "application error after revocation", revoke: true, err: errors.New("application failed")},
	} {
		t.Run(test.name, func(t *testing.T) {
			session := createTestPartitionSession()
			t.Cleanup(session.Close)
			handler := NewMockEventHandler(gomock.NewController(t))
			handler.EXPECT().OnReadMessages(gomock.Any(), gomock.Any()).DoAndReturn(
				func(context.Context, *PublicReadMessages) error {
					if test.revoke {
						session.Close()
					}

					return test.err
				},
			)
			worker := NewPartitionWorker(session.StreamPartitionSessionID, session, newMockMessageSender(), handler,
				func(rawtopicreader.PartitionSessionID, error) {}, &trace.Topic{}, "test-listener")
			err := worker.processUnifiedMessage(xtest.Context(t), unifiedMessage{BatchMessage: &batchMessage{
				ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{Status: rawydb.StatusSuccess},
				Batch:                 createTestBatch(t, session),
			}})
			require.ErrorIs(t, err, test.err)
		})
	}
}
