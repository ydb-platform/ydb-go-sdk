package topicreaderinternal

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
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
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

		err := c.Commit(ctx, topicreadercommon.CommitRange{})
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestCommitterCommitDisabled(t *testing.T) {
	ctx := xtest.Context(t)
	c := &committer{mode: CommitModeNone}
	err := c.Commit(ctx, topicreadercommon.CommitRange{})
	require.ErrorIs(t, err, ErrCommitDisabled)
}

func TestCommitterCommitAsync(t *testing.T) {
	t.Run("SendCommit", func(t *testing.T) {
		ctx := xtest.Context(t)
		session := newTestPartitionSession(context.Background(), 1)

		cRange := topicreadercommon.CommitRange{
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

		cRange := topicreadercommon.CommitRange{
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

		cRange := topicreadercommon.CommitRange{
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
		session.SetCommittedOffset(2)

		cRange := topicreadercommon.CommitRange{
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
		session.SetCommittedOffset(1)
		cRange := topicreadercommon.CommitRange{
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
		require.ErrorIs(t, commitErr, PublicErrCommitSessionToExpiredSession)
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

		_, err := c.pushCommit(topicreadercommon.CommitRange{PartitionSession: newTestPartitionSession(
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

		_, err := c.pushCommit(topicreadercommon.CommitRange{PartitionSession: newTestPartitionSession(
			context.Background(), 1,
		)})
		require.NoError(t, err)
		_, err = c.pushCommit(topicreadercommon.CommitRange{PartitionSession: newTestPartitionSession(
			context.Background(), 2,
		)})
		require.NoError(t, err)
		require.False(t, isSended())

		clock.BlockUntil(1)

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
		c.commits.AppendCommitRanges([]topicreadercommon.CommitRange{
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

		_, err := c.pushCommit(topicreadercommon.CommitRange{PartitionSession: newTestPartitionSession(
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

		for i := 0; i < 3; i++ {
			_, err := c.pushCommit(
				topicreadercommon.CommitRange{
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

		_, err := c.pushCommit(topicreadercommon.CommitRange{PartitionSession: newTestPartitionSession(
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
		_, err := c.pushCommit(topicreadercommon.CommitRange{PartitionSession: newTestPartitionSession(
			context.Background(), 0,
		)})
		require.NoError(t, err)

		clock.BlockUntil(1)
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
		c.commits.AppendCommitRange(topicreadercommon.CommitRange{PartitionSession: newTestPartitionSession(
			context.Background(), 0,
		)})
		require.NoError(t, c.Close(ctx, nil))
		require.True(t, sendCalled)
	})
}

func newTestCommitter(ctx context.Context, t testing.TB) *committer {
	res := newCommitterStopped(&trace.Topic{}, ctx, CommitModeAsync, func(msg rawtopicreader.ClientMessage) error {
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
) *topicreadercommon.PartitionSession {
	return topicreadercommon.NewPartitionSession(
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

func testNewCommitRanges(commitable ...topicreadercommon.PublicCommitRangeGetter) *topicreadercommon.CommitRanges {
	var res topicreadercommon.CommitRanges
	res.Append(commitable...)

	return &res
}
