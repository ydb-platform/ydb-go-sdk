package topiclistenerinternal

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
)

func TestPublicReadMessagesConfirmWithAckAfterFailedConfirm(t *testing.T) {
	session := createTestPartitionSession()
	batch, err := topicreadercommon.NewBatch(session, nil)
	require.NoError(t, err)

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
	session := createTestPartitionSession()
	batch, err := topicreadercommon.NewBatch(session, nil)
	require.NoError(t, err)

	commitErr := errors.New("commit failed")
	syncCommitter := NewMockSyncCommitter(gomock.NewController(t))
	syncCommitter.EXPECT().Commit(gomock.Any(), gomock.Any()).AnyTimes().Return(commitErr)
	commitHandler := NewMockCommitHandler(gomock.NewController(t))
	commitHandler.EXPECT().sendCommit(batch).Return(nil)
	commitHandler.EXPECT().getSyncCommitter().AnyTimes().Return(syncCommitter)
	event := NewPublicReadMessages(session.ToPublic(), batch, commitHandler)

	event.Confirm()

	require.ErrorIs(t, event.ConfirmWithAck(context.Background()), commitErr)
}
