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

func TestPartitionWorkerStopCallbackReportsFailedSuccessorSession(t *testing.T) {
	for _, test := range []struct {
		name  string
		batch bool
		panic bool
	}{
		{name: "start error"},
		{name: "start panic", panic: true},
		{name: "read error", batch: true},
		{name: "read panic", batch: true, panic: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := xtest.Context(t)
			firstSession := createTestPartitionSession()
			secondSession := createTestPartitionSession()
			secondSession.StreamPartitionSessionID++
			t.Cleanup(firstSession.Close)
			t.Cleanup(secondSession.Close)
			handlerErr := errors.New("successor session failed")
			fail := func() error {
				if test.panic {
					panic(handlerErr)
				}

				return handlerErr
			}
			handler := NewMockEventHandler(gomock.NewController(t))
			firstStart := handler.EXPECT().OnStartPartitionSessionRequest(gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, event *PublicEventStartPartitionSession) error {
					event.Confirm()

					return nil
				})
			if test.batch {
				handler.EXPECT().OnReadMessages(gomock.Any(), gomock.Any()).After(firstStart.Call).
					DoAndReturn(func(context.Context, *PublicReadMessages) error { return fail() })
			} else {
				handler.EXPECT().OnStartPartitionSessionRequest(gomock.Any(), gomock.Any()).After(firstStart.Call).
					DoAndReturn(func(context.Context, *PublicEventStartPartitionSession) error { return fail() })
			}
			type stoppedWorker struct {
				sessionID rawtopicreader.PartitionSessionID
				err       error
			}
			stopped := make(chan stoppedWorker, 1)
			worker := NewPartitionWorker(firstSession.StreamPartitionSessionID, firstSession,
				newMockMessageSender(), handler,
				func(sessionID rawtopicreader.PartitionSessionID, err error) {
					stopped <- stoppedWorker{sessionID: sessionID, err: err}
				}, &trace.Topic{}, "test-listener")
			worker.Start(ctx)
			t.Cleanup(func() { require.NoError(t, worker.Close(ctx, nil)) })
			worker.AddRawServerMessage(firstSession, createTestStartPartitionRequest())
			if test.batch {
				worker.AddMessagesBatch(rawtopiccommon.ServerMessageMetadata{Status: rawydb.StatusSuccess},
					createTestBatch(t, secondSession))
			} else {
				worker.AddRawServerMessage(secondSession, createTestStartPartitionRequest())
			}

			got := xtest.Receive(t, stopped, "the worker failure in its successor session")
			if test.panic {
				require.ErrorContains(t, got.err, "partition worker panic: "+handlerErr.Error())
			} else {
				require.ErrorIs(t, got.err, handlerErr)
			}
			require.Equal(t, secondSession.StreamPartitionSessionID, got.sessionID,
				"the stop callback must identify the failed session, rather than the worker's creating session")
		})
	}
}

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
