package topiclistenerinternal

import (
	"context"
	"errors"
	"fmt"
	"sync"
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
	commitHandler.EXPECT().newCommitRequest(batch).Return(&testCommitRequest{
		onSent: func(context.Context) error { return context.Canceled },
		onWait: func(context.Context) error {
			return topicreadercommon.ErrPublicCommitSessionToExpiredSession
		},
	})
	event := NewPublicReadMessages(session.ToPublic(), batch, commitHandler)

	session.Close()
	event.Confirm()

	require.ErrorIs(
		t,
		event.ConfirmWithAck(context.Background()),
		topicreadercommon.ErrPublicCommitSessionToExpiredSession,
	)
}

func TestPublicReadMessagesConfirmWithAckReturnsFailedConfirm(t *testing.T) {
	batch := createTestBatchWithBufferBytes(t, 1)
	session := topicreadercommon.BatchGetPartitionSession(batch)
	commitRange := topicreadercommon.GetCommitRange(batch)
	commitHandler := NewMockCommitHandler(gomock.NewController(t))
	sendErr := errors.New("send failed")
	waits := 0
	request := &testCommitRequest{
		onSent: func(context.Context) error { return sendErr },
		onWait: func(ctx context.Context) error {
			waits++

			return sendErr
		},
	}
	commitHandler.EXPECT().newCommitRequest(batch).Return(request)
	event := NewPublicReadMessages(session.ToPublic(), batch, commitHandler)
	event.Confirm()
	event.Confirm()

	require.ErrorIs(t, event.ConfirmWithAck(context.Background()), sendErr)
	require.ErrorIs(t, event.ConfirmWithAck(context.Background()), sendErr)
	require.EqualValues(t, 1, request.starts.Load())
	require.Equal(t, 2, waits)
	require.Less(t, session.CommittedOffset(), commitRange.CommitOffsetEnd)
}

func TestPublicReadMessagesConfirmWithAckWaitsForConcurrentConfirmError(t *testing.T) {
	ctx := xtest.Context(t)
	batch := createTestBatchWithBufferBytes(t, 1)
	session := topicreadercommon.BatchGetPartitionSession(batch)
	sendErr := errors.New("send failed")
	started, release := make(chan struct{}), make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	commitHandler := NewMockCommitHandler(gomock.NewController(t))
	request := &testCommitRequest{
		onSent: func(context.Context) error {
			close(started)
			<-release

			return sendErr
		},
		onWait: func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-release:
				return sendErr
			}
		},
	}
	commitHandler.EXPECT().newCommitRequest(batch).Return(request)
	event := NewPublicReadMessages(session.ToPublic(), batch, commitHandler)
	confirmDone := make(chan struct{})
	go func() {
		event.Confirm()
		close(confirmDone)
	}()
	<-started
	result := make(chan error, 1)
	go func() { result <- event.ConfirmWithAck(ctx) }()
	select {
	case err := <-result:
		t.Fatalf("ConfirmWithAck returned before Confirm finished: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	released = true
	<-confirmDone
	require.ErrorIs(t, <-result, sendErr)
}

func TestPublicReadMessagesConfirmWithAckAfterConfirm(t *testing.T) {
	batch := createTestBatchWithBufferBytes(t, 1)
	session := topicreadercommon.BatchGetPartitionSession(batch)

	commitErr := errors.New("commit failed")
	waits := 0
	request := &testCommitRequest{onWait: func(context.Context) error {
		waits++

		return commitErr
	}}
	commitHandler := NewMockCommitHandler(gomock.NewController(t))
	commitHandler.EXPECT().newCommitRequest(batch).Return(request)
	event := NewPublicReadMessages(session.ToPublic(), batch, commitHandler)

	event.Confirm()

	require.ErrorIs(t, event.ConfirmWithAck(context.Background()), commitErr)
	require.ErrorIs(t, event.ConfirmWithAck(context.Background()), commitErr)
	require.Equal(t, 2, waits)
	require.EqualValues(t, 1, request.starts.Load())
}

func TestPublicReadMessagesConfirmWithAckWaitAckCancellationDoesNotPoisonEvent(t *testing.T) {
	batch := createTestBatchWithBufferBytes(t, 1)
	session := topicreadercommon.BatchGetPartitionSession(batch)
	commitHandler := NewMockCommitHandler(gomock.NewController(t))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waits := 0
	request := &testCommitRequest{onWait: func(context.Context) error {
		waits++
		if waits == 1 {
			cancel()

			return ctx.Err()
		}
		session.SetCommittedOffsetForward(topicreadercommon.GetCommitRange(batch).CommitOffsetEnd)

		return nil
	}}
	commitHandler.EXPECT().newCommitRequest(batch).Return(request)
	event := NewPublicReadMessages(session.ToPublic(), batch, commitHandler)
	event.Confirm()

	require.ErrorIs(t, event.ConfirmWithAck(ctx), context.Canceled)
	require.NoError(t, event.ConfirmWithAck(context.Background()))
	require.Equal(t, topicreadercommon.GetCommitRange(batch).CommitOffsetEnd, session.CommittedOffset())
	require.EqualValues(t, 1, request.starts.Load())
}

func TestPublicReadMessagesConfirmWithAckSkipsAcknowledgedBatch(t *testing.T) {
	for _, confirmFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("ConfirmFirst=%t", confirmFirst), func(t *testing.T) {
			batch := createTestBatchWithBufferBytes(t, 1)
			session := topicreadercommon.BatchGetPartitionSession(batch)
			commitHandler := NewMockCommitHandler(gomock.NewController(t))
			commitRange := topicreadercommon.GetCommitRange(batch)
			request := &testCommitRequest{onWait: func(ctx context.Context) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				session.SetCommittedOffsetForward(commitRange.CommitOffsetEnd)

				return nil
			}}
			commitHandler.EXPECT().newCommitRequest(batch).Return(request)
			event := NewPublicReadMessages(session.ToPublic(), batch, commitHandler)
			if confirmFirst {
				event.Confirm()
			}

			require.NoError(t, event.ConfirmWithAck(context.Background()))
			require.NoError(t, event.ConfirmWithAck(context.Background()))
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			require.ErrorIs(t, event.ConfirmWithAck(ctx), context.Canceled)
			session.Close()
			require.NoError(t, event.ConfirmWithAck(context.Background()))
			require.EqualValues(t, 1, request.starts.Load())
		})
	}
}

func TestPublicReadMessagesConfirmWithAckAfterAcknowledgedConfirm(t *testing.T) {
	batch := createTestBatchWithBufferBytes(t, 1)
	session := topicreadercommon.BatchGetPartitionSession(batch)
	commitHandler := NewMockCommitHandler(gomock.NewController(t))
	request := &testCommitRequest{}
	commitHandler.EXPECT().newCommitRequest(batch).Return(request)
	event := NewPublicReadMessages(session.ToPublic(), batch, commitHandler)
	event.Confirm()
	session.SetCommittedOffsetForward(topicreadercommon.GetCommitRange(batch).CommitOffsetEnd)

	require.NoError(t, event.ConfirmWithAck(context.Background()))
}

func TestPublicReadMessagesConfirmWithAckWaitsWithoutResendingAfterConfirm(t *testing.T) {
	batch := createTestBatchWithBufferBytes(t, 1)
	session := topicreadercommon.BatchGetPartitionSession(batch)
	ack := make(chan struct{})
	request := &testCommitRequest{onWait: func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ack:
			return nil
		}
	}}
	commitHandler := NewMockCommitHandler(gomock.NewController(t))
	commitHandler.EXPECT().newCommitRequest(batch).Return(request)
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
	require.EqualValues(t, 1, request.starts.Load(), "ConfirmWithAck must not resend Confirm's commit")
	session.SetCommittedOffsetForward(topicreadercommon.GetCommitRange(batch).CommitOffsetEnd)
	close(ack)
	require.NoError(t, <-result)
}

func TestPublicReadMessagesConfirmWithAckConcurrentCallsSendOneCommit(t *testing.T) {
	ctx := xtest.Context(t)
	batch := createTestBatchWithBufferBytes(t, 1)
	session := topicreadercommon.BatchGetPartitionSession(batch)
	started, release := make(chan struct{}), make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	request := &testCommitRequest{
		onStart: func() { close(started) },
		onWait: func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-release:
				return nil
			}
		},
	}
	commitHandler := NewMockCommitHandler(gomock.NewController(t))
	commitHandler.EXPECT().newCommitRequest(batch).Return(request)
	event := NewPublicReadMessages(session.ToPublic(), batch, commitHandler)
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { first <- event.ConfirmWithAck(ctx) }()
	<-started
	go func() { second <- event.ConfirmWithAck(ctx) }()
	select {
	case err := <-second:
		t.Fatalf("second confirmation returned before the first commit completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	require.EqualValues(t, 1, request.starts.Load())
	session.SetCommittedOffsetForward(topicreadercommon.GetCommitRange(batch).CommitOffsetEnd)
	close(release)
	released = true
	require.NoError(t, <-first)
	require.NoError(t, <-second)
	require.EqualValues(t, 1, request.starts.Load())
}

func TestPublicReadMessagesConfirmWithAckConcurrentCallsReturnFirstError(t *testing.T) {
	ctx := xtest.Context(t)
	batch := createTestBatchWithBufferBytes(t, 1)
	session := topicreadercommon.BatchGetPartitionSession(batch)
	commitErr := errors.New("commit failed")
	started, release := make(chan struct{}), make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	request := &testCommitRequest{
		onStart: func() { close(started) },
		onWait: func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-release:
				return commitErr
			}
		},
	}
	commitHandler := NewMockCommitHandler(gomock.NewController(t))
	commitHandler.EXPECT().newCommitRequest(batch).Return(request)
	event := NewPublicReadMessages(session.ToPublic(), batch, commitHandler)
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { first <- event.ConfirmWithAck(ctx) }()
	<-started
	go func() { second <- event.ConfirmWithAck(ctx) }()
	close(release)
	released = true

	require.ErrorIs(t, <-first, commitErr)
	require.ErrorIs(t, <-second, commitErr)
	require.EqualValues(t, 1, request.starts.Load())
}

func TestPublicReadMessagesConfirmWithAckCallerCancellationDoesNotPoisonEvent(t *testing.T) {
	batch := createTestBatchWithBufferBytes(t, 1)
	session := topicreadercommon.BatchGetPartitionSession(batch)
	commitRange := topicreadercommon.GetCommitRange(batch)
	commitHandler := NewMockCommitHandler(gomock.NewController(t))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waits := 0
	request := &testCommitRequest{onWait: func(context.Context) error {
		waits++
		if waits == 1 {
			cancel()

			return ctx.Err()
		}
		session.SetCommittedOffsetForward(commitRange.CommitOffsetEnd)

		return nil
	}}
	commitHandler.EXPECT().newCommitRequest(batch).Return(request)
	event := NewPublicReadMessages(session.ToPublic(), batch, commitHandler)

	require.ErrorIs(t, event.ConfirmWithAck(ctx), context.Canceled)
	require.NoError(t, event.ConfirmWithAck(context.Background()))
	require.Equal(t, commitRange.CommitOffsetEnd, session.CommittedOffset())
	require.EqualValues(t, 1, request.starts.Load())
}

func TestPublicReadMessagesConfirmDoesNotWaitForConcurrentAck(t *testing.T) {
	ctx := xtest.Context(t)
	batch := createTestBatchWithBufferBytes(t, 1)
	session := topicreadercommon.BatchGetPartitionSession(batch)
	started, release := make(chan struct{}), make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	request := &testCommitRequest{
		onStart: func() { close(started) },
		onWait: func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-release:
				return nil
			}
		},
	}
	commitHandler := NewMockCommitHandler(gomock.NewController(t))
	commitHandler.EXPECT().newCommitRequest(batch).Return(request)
	event := NewPublicReadMessages(session.ToPublic(), batch, commitHandler)
	first := make(chan error, 1)
	go func() { first <- event.ConfirmWithAck(ctx) }()
	<-started

	confirmDone := make(chan struct{})
	go func() {
		event.Confirm()
		close(confirmDone)
	}()
	xtest.Receive(t, confirmDone, "Confirm during an in-flight ConfirmWithAck")
	require.EqualValues(t, 1, request.starts.Load())

	session.SetCommittedOffsetForward(topicreadercommon.GetCommitRange(batch).CommitOffsetEnd)
	close(release)
	released = true
	require.NoError(t, <-first)
}

type testCommitRequest struct {
	startOnce sync.Once
	starts    atomic.Int32
	onStart   func()
	onSent    func(context.Context) error
	onWait    func(context.Context) error
}

func (r *testCommitRequest) start() (started bool) {
	r.startOnce.Do(func() {
		started = true
		r.starts.Add(1)
		if r.onStart != nil {
			r.onStart()
		}
	})

	return started
}

func (r *testCommitRequest) Confirm() {
	if r.start() {
		_ = r.waitSent(context.Background())
	}
}

func (r *testCommitRequest) waitSent(ctx context.Context) error {
	if r.onSent != nil {
		return r.onSent(ctx)
	}

	return nil
}

func (r *testCommitRequest) Wait(ctx context.Context) error {
	r.start()
	if r.onWait != nil {
		return r.onWait(ctx)
	}

	return r.waitSent(ctx)
}
