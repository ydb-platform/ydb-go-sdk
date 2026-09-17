package topiclistenerinternal

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
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

func TestPublicReadMessagesConfirmWithAckRetriesFailedConfirm(t *testing.T) {
	batch := createTestBatchWithBufferBytes(t, 1)
	session := topicreadercommon.BatchGetPartitionSession(batch)
	commitRange := topicreadercommon.GetCommitRange(batch)
	commitHandler := NewMockCommitHandler(gomock.NewController(t))
	syncCommitter := NewMockSyncCommitter(gomock.NewController(t))
	commitHandler.EXPECT().sendCommit(batch).Return(errors.New("send failed"))
	commitHandler.EXPECT().getSyncCommitter().Return(syncCommitter)
	syncCommitter.EXPECT().Commit(gomock.Any(), commitRange).DoAndReturn(
		func(context.Context, topicreadercommon.CommitRange) error {
			session.SetCommittedOffsetForward(commitRange.CommitOffsetEnd)

			return nil
		},
	)
	event := NewPublicReadMessages(session.ToPublic(), batch, commitHandler)
	event.Confirm()

	require.NoError(t, event.ConfirmWithAck(context.Background()))
	require.Equal(t, commitRange.CommitOffsetEnd, session.CommittedOffset())
}

func TestPublicReadMessagesConfirmWithAckAfterConfirm(t *testing.T) {
	batch := createTestBatchWithBufferBytes(t, 1)
	session := topicreadercommon.BatchGetPartitionSession(batch)

	commitErr := errors.New("commit failed")
	syncCommitter := NewMockSyncCommitter(gomock.NewController(t))
	syncCommitter.EXPECT().WaitAck(gomock.Any(), gomock.Any()).Times(2).Return(commitErr)
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
			acknowledge := func(context.Context, topicreadercommon.CommitRange) error {
				session.SetCommittedOffsetForward(commitRange.CommitOffsetEnd)

				return nil
			}
			if confirmFirst {
				syncCommitter.EXPECT().WaitAck(gomock.Any(), commitRange).DoAndReturn(acknowledge)
			} else {
				syncCommitter.EXPECT().Commit(gomock.Any(), commitRange).DoAndReturn(acknowledge)
			}
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

func TestPublicReadMessagesConfirmWithAckWaitsWithoutResendingAfterConfirm(t *testing.T) {
	batch := createTestBatchWithBufferBytes(t, 1)
	session := topicreadercommon.BatchGetPartitionSession(batch)
	committer := &ackOnlySyncCommitter{ack: make(chan struct{})}
	commitHandler := NewMockCommitHandler(gomock.NewController(t))
	commitHandler.EXPECT().sendCommit(batch).Return(nil)
	commitHandler.EXPECT().getSyncCommitter().Return(committer)
	event := NewPublicReadMessages(session.ToPublic(), batch, commitHandler)
	event.Confirm()

	result := make(chan error, 1)
	go func() {
		result <- event.ConfirmWithAck(context.Background())
	}()
	select {
	case err := <-result:
		t.Fatalf("ConfirmWithAck returned before the commit ACK: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	require.Zero(t, committer.commits.Load(), "ConfirmWithAck must not resend Confirm's commit")
	session.SetCommittedOffsetForward(topicreadercommon.GetCommitRange(batch).CommitOffsetEnd)
	close(committer.ack)
	require.NoError(t, <-result)
}

func TestPublicReadMessagesConfirmWithAckConcurrentCallsSendOneCommit(t *testing.T) {
	ctx := xtest.Context(t)
	batch := createTestBatchWithBufferBytes(t, 1)
	session := topicreadercommon.BatchGetPartitionSession(batch)
	committer := &blockingSyncCommitter{started: make(chan struct{}), release: make(chan struct{})}
	released := false
	defer func() {
		if !released {
			close(committer.release)
		}
	}()
	commitHandler := NewMockCommitHandler(gomock.NewController(t))
	commitHandler.EXPECT().getSyncCommitter().Return(committer)
	event := NewPublicReadMessages(session.ToPublic(), batch, commitHandler)
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { first <- event.ConfirmWithAck(ctx) }()
	<-committer.started
	go func() { second <- event.ConfirmWithAck(ctx) }()
	select {
	case err := <-second:
		t.Fatalf("second confirmation returned before the first commit completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	require.EqualValues(t, 1, committer.commits.Load())
	session.SetCommittedOffsetForward(topicreadercommon.GetCommitRange(batch).CommitOffsetEnd)
	close(committer.release)
	released = true
	require.NoError(t, <-first)
	require.NoError(t, <-second)
	require.EqualValues(t, 1, committer.commits.Load())
}

type ackOnlySyncCommitter struct {
	commits atomic.Int32
	ack     chan struct{}
}

func (c *ackOnlySyncCommitter) Commit(context.Context, topicreadercommon.CommitRange) error {
	c.commits.Add(1)

	return nil
}

func (c *ackOnlySyncCommitter) WaitAck(ctx context.Context, _ topicreadercommon.CommitRange) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.ack:
		return nil
	}
}

type blockingSyncCommitter struct {
	commits atomic.Int32
	started chan struct{}
	release chan struct{}
}

func (c *blockingSyncCommitter) Commit(ctx context.Context, _ topicreadercommon.CommitRange) error {
	if c.commits.Add(1) == 1 {
		close(c.started)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.release:
		return nil
	}
}

func (c *blockingSyncCommitter) WaitAck(context.Context, topicreadercommon.CommitRange) error {
	return nil
}
