package topicreadercommon

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	xtest "github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestCommitterCommit(t *testing.T) {
	t.Run("CommitWithCancelledContext", func(t *testing.T) {
		ctx := xtest.Context(t)
		c := newTestCommitter(ctx, t)
		c.send = func(msg rawtopicreader.ClientMessage) error {
			t.Fatalf("must not call")

			return nil
		}

		ctx, cancel := xcontext.WithCancel(ctx)
		cancel()

		err := c.Commit(ctx, CommitRange{})
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("ExpiredSession", func(t *testing.T) {
		ctx := xtest.Context(t)
		committerCtx, cancelCommitter := context.WithCancel(ctx)
		committer := NewCommitterStopped(&trace.Topic{}, committerCtx, CommitModeSync, nil)
		session := newTestPartitionSession(ctx, 1)

		cancelCommitter()
		session.Close()

		err := committer.Commit(ctx, CommitRange{PartitionSession: session})
		require.ErrorIs(t, err, ErrPublicCommitSessionToExpiredSession)
	})
}

func TestCommitRequestKeepsSendError(t *testing.T) {
	ctx := xtest.Context(t)
	session := newTestPartitionSession(ctx, 1)
	commitRange := CommitRange{PartitionSession: session, CommitOffsetStart: 1, CommitOffsetEnd: 2}
	sendErr := errors.New("send failed")
	sends := 0
	committer := NewCommitterStopped(&trace.Topic{}, ctx, CommitModeSync,
		func(rawtopicreader.ClientMessage) error {
			sends++

			return sendErr
		})
	committer.Start()
	defer func() { _ = committer.Close(ctx, nil) }()
	request := committer.NewCommitRequest(commitRange)

	request.Confirm()
	request.Confirm()
	require.ErrorIs(t, request.Wait(ctx), sendErr)
	require.Equal(t, 1, sends)
}

func TestCommitRequestKeepsSendErrorAfterSessionClose(t *testing.T) {
	ctx := xtest.Context(t)
	session := newTestPartitionSession(ctx, 1)
	sendErr := errors.New("send failed")
	committer := NewCommitterStopped(&trace.Topic{}, ctx, CommitModeSync,
		func(rawtopicreader.ClientMessage) error { return sendErr })
	committer.Start()
	defer func() { _ = committer.Close(ctx, nil) }()
	request := committer.NewCommitRequest(CommitRange{
		PartitionSession: session, CommitOffsetStart: 1, CommitOffsetEnd: 2,
	})

	request.Confirm()
	session.Close()

	require.ErrorIs(t, request.Wait(ctx), sendErr)
}

func TestCommitRequestAcknowledgedBeforeSessionCloseSucceeds(t *testing.T) {
	ctx := xtest.Context(t)
	session := newTestPartitionSession(ctx, 1)
	committer := NewCommitterStopped(&trace.Topic{}, ctx, CommitModeSync,
		func(rawtopicreader.ClientMessage) error {
			t.Fatal("acknowledged commit must not be resent")

			return nil
		})
	committer.Start()
	defer func() { _ = committer.Close(ctx, nil) }()
	request := committer.NewCommitRequest(CommitRange{
		PartitionSession: session, CommitOffsetStart: 1, CommitOffsetEnd: 2,
	})
	session.SetCommittedOffsetForward(2)
	session.Close()

	require.NoError(t, request.Wait(ctx))
}

func TestCommitRequestQueuedDuringFailedFlushCompletes(t *testing.T) {
	ctx := xtest.Context(t)
	session := newTestPartitionSession(ctx, 1)
	sendErr := errors.New("commit send failed")
	sendStarted := make(chan struct{})
	releaseSend := make(chan struct{})
	committer := NewCommitterStopped(&trace.Topic{}, ctx, CommitModeSync,
		func(rawtopicreader.ClientMessage) error {
			close(sendStarted)
			<-releaseSend

			return sendErr
		})
	committer.Start()
	defer func() { _ = committer.Close(ctx, nil) }()

	first := committer.NewCommitRequest(CommitRange{
		PartitionSession: session, CommitOffsetStart: 1, CommitOffsetEnd: 2,
	})
	firstDone := make(chan struct{})
	go func() {
		first.Confirm()
		close(firstDone)
	}()
	<-sendStarted

	second := committer.NewCommitRequest(CommitRange{
		PartitionSession: session, CommitOffsetStart: 2, CommitOffsetEnd: 3,
	})
	waitCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	secondResult := make(chan error, 1)
	go func() { secondResult <- second.Wait(waitCtx) }()
	require.Eventually(t, func() bool {
		var queued bool
		committer.m.WithLock(func() { queued = len(committer.requests) == 1 })

		return queued
	}, time.Second, time.Millisecond)

	close(releaseSend)
	<-firstDone
	require.ErrorIs(t, first.Wait(ctx), sendErr)
	require.ErrorIs(t, <-secondResult, sendErr)
}

func TestCommitRequestCancellationDoesNotResend(t *testing.T) {
	ctx := xtest.Context(t)
	session := newTestPartitionSession(ctx, 1)
	commitRange := CommitRange{PartitionSession: session, CommitOffsetStart: 1, CommitOffsetEnd: 2}
	sendStarted := make(chan struct{})
	releaseSend := make(chan struct{})
	sends := 0
	committer := NewCommitterStopped(&trace.Topic{}, ctx, CommitModeSync,
		func(rawtopicreader.ClientMessage) error {
			sends++
			close(sendStarted)
			<-releaseSend

			return nil
		})
	committer.Start()
	defer func() { _ = committer.Close(ctx, nil) }()
	request := committer.NewCommitRequest(commitRange)
	confirmDone := make(chan struct{})
	go func() {
		request.Confirm()
		close(confirmDone)
	}()
	<-sendStarted
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, request.Wait(cancelled), context.Canceled)
	close(releaseSend)
	<-confirmDone
	session.SetCommittedOffsetForward(commitRange.CommitOffsetEnd)
	committer.OnCommitNotify(session, commitRange.CommitOffsetEnd)
	require.NoError(t, request.Wait(ctx))
	require.Equal(t, 1, sends)
}

func TestCommitRequestAlreadyAcknowledgedDoesNotSend(t *testing.T) {
	ctx := xtest.Context(t)
	session := newTestPartitionSession(ctx, 1)
	commitRange := CommitRange{PartitionSession: session, CommitOffsetStart: 1, CommitOffsetEnd: 2}
	session.SetCommittedOffsetForward(commitRange.CommitOffsetEnd)
	committer := NewCommitterStopped(&trace.Topic{}, ctx, CommitModeSync,
		func(rawtopicreader.ClientMessage) error {
			t.Fatal("already acknowledged commit must not be sent")

			return nil
		})
	committer.Start()
	defer func() { _ = committer.Close(ctx, nil) }()
	request := committer.NewCommitRequest(commitRange)

	require.NoError(t, request.Wait(ctx))
	request.Confirm()
	require.NoError(t, request.Wait(ctx))
}

func TestCommitRequestConcurrentWaitsShareSend(t *testing.T) {
	ctx := xtest.Context(t)
	session := newTestPartitionSession(ctx, 1)
	commitRange := CommitRange{PartitionSession: session, CommitOffsetStart: 1, CommitOffsetEnd: 2}
	sends := 0
	committer := NewCommitterStopped(&trace.Topic{}, ctx, CommitModeSync,
		func(rawtopicreader.ClientMessage) error {
			sends++

			return nil
		})
	committer.Start()
	defer func() { _ = committer.Close(ctx, nil) }()
	request := committer.NewCommitRequest(commitRange)
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { first <- request.Wait(ctx) }()
	go func() { second <- request.Wait(ctx) }()
	require.Eventually(t, func() bool {
		var waiting bool
		committer.m.WithLock(func() {
			waiting = len(committer.waiters) == 2
		})

		return waiting
	}, time.Second, time.Millisecond)
	session.SetCommittedOffsetForward(commitRange.CommitOffsetEnd)
	committer.OnCommitNotify(session, commitRange.CommitOffsetEnd)
	require.NoError(t, <-first)
	require.NoError(t, <-second)
	require.Equal(t, 1, sends)
}

func TestCommitRequestClosedCommitterReturnsExpiredSession(t *testing.T) {
	ctx := xtest.Context(t)
	committerCtx, cancelCommitter := context.WithCancel(ctx)
	committer := NewCommitterStopped(&trace.Topic{}, committerCtx, CommitModeSync,
		func(rawtopicreader.ClientMessage) error {
			t.Fatal("commit must not be sent after the committer has closed")

			return nil
		})
	cancelCommitter()
	session := newTestPartitionSession(ctx, 1)
	request := committer.NewCommitRequest(CommitRange{
		PartitionSession:  session,
		CommitOffsetStart: 1,
		CommitOffsetEnd:   2,
	})

	request.Confirm()
	require.NoError(t, session.Context().Err(), "partition session may still be open during shutdown")
	require.ErrorIs(t, request.Wait(ctx), ErrPublicCommitSessionToExpiredSession)
}

func TestCommitRequestCloseDuringSendReturnsSendError(t *testing.T) {
	ctx := xtest.Context(t)
	committerCtx, cancelCommitter := context.WithCancel(ctx)
	sendErr := errors.New("closed stream")
	committer := NewCommitterStopped(&trace.Topic{}, committerCtx, CommitModeSync,
		func(rawtopicreader.ClientMessage) error { return sendErr })
	session := newTestPartitionSession(ctx, 1)
	request := committer.NewCommitRequest(CommitRange{
		PartitionSession:  session,
		CommitOffsetStart: 1,
		CommitOffsetEnd:   2,
	})
	result := make(chan error, 1)
	go func() { result <- request.Wait(ctx) }()
	require.Eventually(t, func() bool {
		var queued bool
		committer.m.WithLock(func() {
			queued = len(committer.requests) == 1
		})

		return queued
	}, time.Second, time.Millisecond)
	cancelCommitter()
	require.ErrorIs(t, committer.Flush(), sendErr)
	require.NoError(t, session.Context().Err())
	require.ErrorIs(t, <-result, sendErr)
}

func TestCommitterCommitDisabled(t *testing.T) {
	ctx := xtest.Context(t)
	c := &Committer{mode: CommitModeNone}
	err := c.Commit(ctx, CommitRange{})
	require.ErrorIs(t, err, ErrCommitDisabled)
}

func TestCommitRequestWaitRejectsMissingSession(t *testing.T) {
	ctx := xtest.Context(t)
	committer := NewCommitterStopped(&trace.Topic{}, ctx, CommitModeSync, nil)

	require.ErrorIs(t, committer.NewCommitRequest(CommitRange{}).Wait(ctx),
		ErrPublicCommitSessionToExpiredSession)
}

func TestCommitRequestWaitRejectsNonSyncModeWithoutQueuing(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode PublicCommitMode
	}{
		{name: "async", mode: CommitModeAsync},
		{name: "none", mode: CommitModeNone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := xtest.Context(t)
			committer := NewCommitterStopped(&trace.Topic{}, ctx, tc.mode, nil)
			session := newTestPartitionSession(ctx, 1)
			commitRange := CommitRange{
				PartitionSession:  session,
				CommitOffsetStart: 1,
				CommitOffsetEnd:   2,
			}

			require.ErrorIs(t, committer.NewCommitRequest(commitRange).Wait(ctx), ErrWaitAckRequiresSyncMode)
			committer.m.WithLock(func() {
				require.Empty(t, committer.waiters)
				require.Empty(t, committer.requests)
				require.Zero(t, committer.commits.Len())
			})
		})
	}
}

func TestCommitterCommitAsync(t *testing.T) {
	t.Run("ExpiredSessionStillQueuesCommit", func(t *testing.T) {
		ctx := xtest.Context(t)
		session := newTestPartitionSession(ctx, 1)
		committer := NewCommitterStopped(&trace.Topic{}, ctx, CommitModeAsync, nil)
		session.Close()

		err := committer.Commit(ctx, CommitRange{
			PartitionSession:  session,
			CommitOffsetStart: 1,
			CommitOffsetEnd:   2,
		})
		require.NoError(t, err)
		committer.m.WithLock(func() {
			require.Equal(t, 1, committer.commits.Len())
		})
	})

	t.Run("SendCommit", func(t *testing.T) {
		ctx := xtest.Context(t)
		session := newTestPartitionSession(context.Background(), 1)

		cRange := CommitRange{
			CommitOffsetStart: 1,
			CommitOffsetEnd:   2,
			PartitionSession:  session,
		}

		sendCalled := make(empty.Chan)
		c := newTestCommitter(ctx, t)
		c.mode = CommitModeAsync
		c.send = func(msg rawtopicreader.ClientMessage) error {
			close(sendCalled)
			require.Equal(t,
				&rawtopicreader.CommitOffsetRequest{
					CommitOffsets: testNewCommitRanges(&cRange).ToPartitionsOffsets(),
				},
				msg)

			return nil
		}
		require.NoError(t, c.Commit(ctx, cRange))
		<-sendCalled
	})
}

func TestCommitterCommitSync(t *testing.T) {
	t.Run("SendCommit", func(t *testing.T) {
		ctx := xtest.Context(t)
		session := newTestPartitionSession(context.Background(), 1)

		cRange := CommitRange{
			CommitOffsetStart: 1,
			CommitOffsetEnd:   2,
			PartitionSession:  session,
		}

		sendCalled := false
		c := newTestCommitter(ctx, t)
		c.mode = CommitModeSync
		c.send = func(msg rawtopicreader.ClientMessage) error {
			sendCalled = true
			require.Equal(t,
				&rawtopicreader.CommitOffsetRequest{
					CommitOffsets: testNewCommitRanges(&cRange).ToPartitionsOffsets(),
				},
				msg)
			c.OnCommitNotify(session, cRange.CommitOffsetEnd)

			return nil
		}
		require.NoError(t, c.Commit(ctx, cRange))
		require.True(t, sendCalled)
	})

	xtest.TestManyTimesWithName(t, "SuccessCommitWithNotifyAfterCommit", func(t testing.TB) {
		ctx := xtest.Context(t)
		session := newTestPartitionSession(context.Background(), 1)

		cRange := CommitRange{
			CommitOffsetStart: 1,
			CommitOffsetEnd:   2,
			PartitionSession:  session,
		}

		commitSended := make(empty.Chan)
		c := newTestCommitter(ctx, t)
		c.mode = CommitModeSync
		c.send = func(msg rawtopicreader.ClientMessage) error {
			close(commitSended)

			return nil
		}

		commitCompleted := make(empty.Chan)
		go func() {
			require.NoError(t, c.Commit(ctx, cRange))
			close(commitCompleted)
		}()

		notifySended := false
		go func() {
			<-commitSended
			notifySended = true
			c.OnCommitNotify(session, rawtopiccommon.Offset(2))
		}()

		<-commitCompleted
		require.True(t, notifySended)
	})

	t.Run("SuccessCommitPreviousCommitted", func(t *testing.T) {
		ctx := xtest.Context(t)
		session := newTestPartitionSession(context.Background(), 1)
		session.SetCommittedOffsetForward(2)

		cRange := CommitRange{
			CommitOffsetStart: 1,
			CommitOffsetEnd:   2,
			PartitionSession:  session,
		}

		c := newTestCommitter(ctx, t)
		require.NoError(t, c.Commit(ctx, cRange))
	})

	xtest.TestManyTimesWithName(t, "SessionClosed", func(t testing.TB) {
		ctx := xtest.Context(t)

		sessionCtx, sessionCancel := xcontext.WithCancel(ctx)

		session := newTestPartitionSession(sessionCtx, 1)
		session.SetCommittedOffsetForward(1)
		cRange := CommitRange{
			CommitOffsetStart: 1,
			CommitOffsetEnd:   2,
			PartitionSession:  session,
		}

		c := newTestCommitter(ctx, t)
		c.mode = CommitModeSync

		waitErr := make(chan error)
		go func() {
			commitErr := c.Commit(ctx, cRange)
			waitErr <- commitErr
		}()

		sessionCancel()

		commitErr := <-waitErr
		require.ErrorIs(t, commitErr, ErrPublicCommitSessionToExpiredSession)
	})
}

func TestCommitterBuffer(t *testing.T) {
	t.Run("SendZeroLag", func(t *testing.T) {
		ctx := xtest.Context(t)
		c := newTestCommitter(ctx, t)

		sendCalled := make(empty.Chan)
		clock := clockwork.NewFakeClock()
		c.clock = clock
		c.send = func(msg rawtopicreader.ClientMessage) error {
			close(sendCalled)

			return nil
		}

		_, err := c.pushCommit(CommitRange{PartitionSession: newTestPartitionSession(
			context.Background(), 2,
		)})
		require.NoError(t, err)
		<-sendCalled
	})
	t.Run("TimeLagTrigger", func(t *testing.T) {
		ctx := xtest.Context(t)
		c := newTestCommitter(ctx, t)

		sendCalled := make(empty.Chan)
		isSended := func() bool {
			select {
			case <-sendCalled:
				return true
			default:
				return false
			}
		}

		clock := clockwork.NewFakeClock()
		c.clock = clock
		c.BufferTimeLagTrigger = time.Second
		c.send = func(msg rawtopicreader.ClientMessage) error {
			commitMess := msg.(*rawtopicreader.CommitOffsetRequest)
			require.Len(t, commitMess.CommitOffsets, 2)
			close(sendCalled)

			return nil
		}

		_, err := c.pushCommit(CommitRange{PartitionSession: newTestPartitionSession(
			context.Background(), 1,
		)})
		require.NoError(t, err)
		_, err = c.pushCommit(CommitRange{PartitionSession: newTestPartitionSession(
			context.Background(), 2,
		)})
		require.NoError(t, err)
		require.False(t, isSended())

		err = clock.BlockUntilContext(ctx, 1)
		require.NoError(t, err)

		clock.Advance(time.Second - 1)
		time.Sleep(time.Millisecond)
		require.False(t, isSended())

		clock.Advance(1)
		<-sendCalled
	})
	t.Run("CountAndTimeFireCountMoreThenNeed", func(t *testing.T) {
		ctx := xtest.Context(t)
		c := newTestCommitter(ctx, t)

		sendCalled := make(empty.Chan)

		clock := clockwork.NewFakeClock()
		c.clock = clock
		c.BufferTimeLagTrigger = time.Second // for prevent send
		c.BufferCountTrigger = 2
		c.send = func(msg rawtopicreader.ClientMessage) error {
			commitMess := msg.(*rawtopicreader.CommitOffsetRequest)
			require.Len(t, commitMess.CommitOffsets, 4)
			close(sendCalled)

			return nil
		}
		c.commits.AppendCommitRanges([]CommitRange{
			{PartitionSession: newTestPartitionSession(
				context.Background(), 1,
			)},
			{PartitionSession: newTestPartitionSession(
				context.Background(), 2,
			)},
			{PartitionSession: newTestPartitionSession(
				context.Background(), 3,
			)},
		})

		_, err := c.pushCommit(CommitRange{PartitionSession: newTestPartitionSession(
			context.Background(), 4,
		)})
		require.NoError(t, err)
		<-sendCalled
	})
	t.Run("CountAndTimeFireCountOnAdd", func(t *testing.T) {
		ctx := xtest.Context(t)
		c := newTestCommitter(ctx, t)

		sendCalled := make(empty.Chan)
		isSended := func() bool {
			select {
			case <-sendCalled:
				return true
			default:
				return false
			}
		}

		clock := clockwork.NewFakeClock()
		c.clock = clock
		c.BufferTimeLagTrigger = time.Second // for prevent send
		c.BufferCountTrigger = 4
		c.send = func(msg rawtopicreader.ClientMessage) error {
			commitMess := msg.(*rawtopicreader.CommitOffsetRequest)
			require.Len(t, commitMess.CommitOffsets, 4)
			close(sendCalled)

			return nil
		}

		for i := range 3 {
			_, err := c.pushCommit(
				CommitRange{
					PartitionSession: newTestPartitionSession(
						context.Background(), rawtopicreader.PartitionSessionID(i),
					),
				},
			)
			require.NoError(t, err)
		}

		// wait notify consumed
		xtest.SpinWaitCondition(t, &c.m, func() bool {
			return len(c.commits.Ranges) == 3
		})
		require.False(t, isSended())

		_, err := c.pushCommit(CommitRange{PartitionSession: newTestPartitionSession(
			context.Background(), 3,
		)})
		require.NoError(t, err)
		<-sendCalled
	})
	t.Run("CountAndTimeFireTime", func(t *testing.T) {
		ctx := xtest.Context(t)
		clock := clockwork.NewFakeClock()
		c := newTestCommitter(ctx, t)
		c.clock = clock
		c.BufferCountTrigger = 2
		c.BufferTimeLagTrigger = time.Second

		sendCalled := make(empty.Chan)
		c.send = func(msg rawtopicreader.ClientMessage) error {
			close(sendCalled)

			return nil
		}
		_, err := c.pushCommit(CommitRange{PartitionSession: newTestPartitionSession(
			context.Background(), 0,
		)})
		require.NoError(t, err)

		err = clock.BlockUntilContext(ctx, 1)
		require.NoError(t, err)
		clock.Advance(time.Second)
		<-sendCalled
	})
	t.Run("FireWithEmptyBuffer", func(t *testing.T) {
		ctx := xtest.Context(t)
		c := newTestCommitter(ctx, t)
		c.send = func(msg rawtopicreader.ClientMessage) error {
			t.Fatal()

			return nil
		}
		c.commitLoopSignal <- empty.Struct{} // to buffer
		c.commitLoopSignal <- empty.Struct{} // if send - first message consumed by send loop
		c.commitLoopSignal <- empty.Struct{} // if send - second message consumed and first processed
	})
	t.Run("FlushOnClose", func(t *testing.T) {
		ctx := xtest.Context(t)
		c := newTestCommitter(ctx, t)

		sendCalled := false
		c.send = func(msg rawtopicreader.ClientMessage) error {
			sendCalled = true

			return nil
		}
		c.commits.AppendCommitRange(CommitRange{PartitionSession: newTestPartitionSession(
			context.Background(), 0,
		)})
		require.NoError(t, c.Close(ctx, nil))
		require.True(t, sendCalled)
	})
}

func newTestCommitter(ctx context.Context, t testing.TB) *Committer {
	res := NewCommitterStopped(&trace.Topic{}, ctx, CommitModeAsync, func(msg rawtopicreader.ClientMessage) error {
		return nil
	})
	res.Start()
	t.Cleanup(func() {
		if err := res.Close(ctx, errors.New("test committer closed")); err != nil {
			require.ErrorIs(t, err, background.ErrAlreadyClosed)
		}
	})

	return res
}

func newTestPartitionSession(
	ctx context.Context,
	partitionSessionID rawtopicreader.PartitionSessionID,
) *PartitionSession {
	return NewPartitionSession(
		ctx,
		"",
		0,
		-1,
		"",
		partitionSessionID,
		int64(partitionSessionID)+100,
		0,
	)
}

func testNewCommitRanges(commitable ...PublicCommitRangeGetter) *CommitRanges {
	var res CommitRanges
	res.Append(commitable...)

	return &res
}
