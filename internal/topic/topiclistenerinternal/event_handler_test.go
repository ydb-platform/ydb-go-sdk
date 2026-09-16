package topiclistenerinternal

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
)

func TestPublicReadMessagesConfirmWithAckAfterFailedConfirm(t *testing.T) {
	batch := createTestBatchWithBufferBytes(t, 1)
	session := topicreadercommon.BatchGetPartitionSession(batch)

	commitHandler := NewMockCommitHandler(gomock.NewController(t))
	commitHandler.EXPECT().sendCommit(batch).Return(context.Canceled)
	event := NewPublicReadMessages(session.ToPublic(), batch, commitHandler)

	session.Close()
	event.Confirm()

	require.ErrorIs(
		t,
		event.ConfirmWithAck(context.Background()),
		topicreadercommon.ErrPublicCommitSessionToExpiredSession,
	)
}

func TestPublicReadMessagesConfirmWithAckAfterConfirm(t *testing.T) {
	batch := createTestBatchWithBufferBytes(t, 1)
	session := topicreadercommon.BatchGetPartitionSession(batch)

	commitErr := errors.New("commit failed")
	syncCommitter := NewMockSyncCommitter(gomock.NewController(t))
	syncCommitter.EXPECT().Commit(gomock.Any(), gomock.Any()).AnyTimes().Return(commitErr)
	commitHandler := NewMockCommitHandler(gomock.NewController(t))
	commitHandler.EXPECT().sendCommit(batch).Return(nil)
	commitHandler.EXPECT().getSyncCommitter().AnyTimes().Return(syncCommitter)
	event := NewPublicReadMessages(session.ToPublic(), batch, commitHandler)

	event.Confirm()

	require.ErrorIs(t, event.ConfirmWithAck(context.Background()), commitErr)
	require.ErrorIs(t, event.ConfirmWithAck(context.Background()), commitErr)
}

func TestPublicReadMessagesConfirmWithAckSkipsAcknowledgedBatch(t *testing.T) {
	for _, confirmFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("ConfirmFirst=%t", confirmFirst), func(t *testing.T) {
			batch := createTestBatchWithBufferBytes(t, 1)
			session := topicreadercommon.BatchGetPartitionSession(batch)
			commitHandler := NewMockCommitHandler(gomock.NewController(t))
			syncCommitter := NewMockSyncCommitter(gomock.NewController(t))
			commitRange := topicreadercommon.GetCommitRange(batch)
			commitHandler.EXPECT().getSyncCommitter().Return(syncCommitter)
			syncCommitter.EXPECT().Commit(gomock.Any(), commitRange).DoAndReturn(
				func(context.Context, topicreadercommon.CommitRange) error {
					session.SetCommittedOffsetForward(commitRange.CommitOffsetEnd)

					return nil
				},
			)
			event := NewPublicReadMessages(session.ToPublic(), batch, commitHandler)
			if confirmFirst {
				commitHandler.EXPECT().sendCommit(batch).Return(nil)
				event.Confirm()
			}

			require.NoError(t, event.ConfirmWithAck(context.Background()))
			require.NoError(t, event.ConfirmWithAck(context.Background()))
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			require.ErrorIs(t, event.ConfirmWithAck(ctx), context.Canceled)
			session.Close()
			require.ErrorIs(t, event.ConfirmWithAck(context.Background()),
				topicreadercommon.ErrPublicCommitSessionToExpiredSession)
		})
	}
}

func TestPublicReadMessagesConfirmWithAckAfterAcknowledgedConfirm(t *testing.T) {
	batch := createTestBatchWithBufferBytes(t, 1)
	session := topicreadercommon.BatchGetPartitionSession(batch)
	commitHandler := NewMockCommitHandler(gomock.NewController(t))
	commitHandler.EXPECT().sendCommit(batch).Return(nil)
	event := NewPublicReadMessages(session.ToPublic(), batch, commitHandler)
	event.Confirm()
	session.SetCommittedOffsetForward(topicreadercommon.GetCommitRange(batch).CommitOffsetEnd)

	require.NoError(t, event.ConfirmWithAck(context.Background()))
}
