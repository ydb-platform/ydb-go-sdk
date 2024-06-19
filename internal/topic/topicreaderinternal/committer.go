package topicreaderinternal

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	ErrCommitDisabled             = xerrors.Wrap(errors.New("ydb: commits disabled"))
	ErrWrongCommitOrderInSyncMode = xerrors.Wrap(errors.New("ydb: wrong commit order in sync mode"))
)

type sendMessageToServerFunc func(msg rawtopicreader.ClientMessage) error

type PublicCommitMode int

const (
	CommitModeAsync PublicCommitMode = iota // default
	CommitModeNone
	CommitModeSync
)

func (m PublicCommitMode) commitsEnabled() bool {
	return m != CommitModeNone
}

type committer struct {
	BufferTimeLagTrigger time.Duration // 0 mean no additional time lag
	BufferCountTrigger   int

	send sendMessageToServerFunc
	mode PublicCommitMode

	clock            clockwork.Clock
	commitLoopSignal empty.Chan
	backgroundWorker background.Worker
	tracer           *trace.Topic

	m       xsync.Mutex
	waiters []commitWaiter
	commits CommitRanges
}

func newCommitterStopped(
	tracer *trace.Topic,
	lifeContext context.Context, //nolint:revive
	mode PublicCommitMode,
	send sendMessageToServerFunc,
) *committer {
	res := &committer{
		mode:             mode,
		clock:            clockwork.NewRealClock(),
		send:             send,
		backgroundWorker: *background.NewWorker(lifeContext, "ydb-topic-reader-committer"),
		tracer:           tracer,
	}
	res.initChannels()

	return res
}

func (c *committer) initChannels() {
	c.commitLoopSignal = make(empty.Chan, 1)
}

func (c *committer) Start() {
	c.backgroundWorker.Start("commit pusher", c.pushCommitsLoop)
}

func (c *committer) Close(ctx context.Context, err error) error {
	return c.backgroundWorker.Close(ctx, err)
}

func (c *committer) Commit(ctx context.Context, commitRange commitRange) error {
	if !c.mode.commitsEnabled() {
		return ErrCommitDisabled
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	waiter, err := c.pushCommit(commitRange)
	if err != nil {
		return err
	}

	return c.waitCommitAck(ctx, waiter)
}

func (c *committer) pushCommit(commitRange commitRange) (commitWaiter, error) {
	var resErr error
	waiter := newCommitWaiter(commitRange.partitionSession, commitRange.commitOffsetEnd)
	c.m.WithLock(func() {
		if err := c.backgroundWorker.Context().Err(); err != nil {
			resErr = err

			return
		}

		c.commits.Append(&commitRange)
		if c.mode == CommitModeSync {
			c.addWaiterNeedLock(waiter)
		}
	})

	select {
	case c.commitLoopSignal <- struct{}{}:
	default:
	}

	return waiter, resErr
}

func (c *committer) pushCommitsLoop(ctx context.Context) {
	for {
		c.waitSendTrigger(ctx)

		var commits CommitRanges
		c.m.WithLock(func() {
			commits = c.commits
			c.commits = NewCommitRangesWithCapacity(commits.len() * 2) //nolint:gomnd
		})

		if commits.len() == 0 && c.backgroundWorker.Context().Err() != nil {
			// committer closed with empty buffer - target close state
			return
		}

		// all ranges already committed of prev iteration
		if commits.len() == 0 {
			continue
		}

		commits.optimize()

		onDone := trace.TopicOnReaderSendCommitMessage(
			c.tracer,
			&commits,
		)
		err := sendCommitMessage(c.send, commits)
		onDone(err)

		if err != nil {
			_ = c.backgroundWorker.Close(ctx, err)
		}
	}
}

func (c *committer) waitSendTrigger(ctx context.Context) {
	ctxDone := ctx.Done()
	select {
	case <-ctxDone:
		return
	case <-c.commitLoopSignal:
	}

	if c.BufferTimeLagTrigger == 0 {
		return
	}

	bufferTimeLagTriggerTimer := c.clock.NewTimer(c.BufferTimeLagTrigger)
	defer bufferTimeLagTriggerTimer.Stop()

	finish := bufferTimeLagTriggerTimer.Chan()
	if c.BufferCountTrigger == 0 {
		select {
		case <-ctxDone:
		case <-finish:
		}

		return
	}

	for {
		var commitsLen int
		c.m.WithLock(func() {
			commitsLen = c.commits.len()
		})
		if commitsLen >= c.BufferCountTrigger {
			return
		}

		select {
		case <-ctxDone:
			return
		case <-finish:
			return
		case <-c.commitLoopSignal:
			// check count on next loop iteration
		}
	}
}

func (c *committer) waitCommitAck(ctx context.Context, waiter commitWaiter) error {
	if c.mode != CommitModeSync {
		return nil
	}

	defer c.m.WithLock(func() {
		c.removeWaiterByIDNeedLock(waiter.ID)
	})
	if waiter.checkCondition(waiter.Session, waiter.Session.committedOffset()) {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-waiter.Session.Context().Done():
		return PublicErrCommitSessionToExpiredSession
	case <-waiter.Committed:
		return nil
	}
}

func (c *committer) OnCommitNotify(session *partitionSession, offset rawtopicreader.Offset) {
	c.m.WithLock(func() {
		for i := range c.waiters {
			waiter := c.waiters[i]
			if waiter.checkCondition(session, offset) {
				select {
				case waiter.Committed <- struct{}{}:
				default:
				}
			}
		}
	})
}

func (c *committer) addWaiterNeedLock(waiter commitWaiter) {
	c.waiters = append(c.waiters, waiter)
}

func (c *committer) removeWaiterByIDNeedLock(id int64) {
	newWaiters := c.waiters[:0]
	for i := range c.waiters {
		if c.waiters[i].ID == id {
			continue
		}

		newWaiters = append(newWaiters, c.waiters[i])
	}
	c.waiters = newWaiters
}

type commitWaiter struct {
	ID        int64
	Session   *partitionSession
	EndOffset rawtopicreader.Offset
	Committed empty.Chan
}

func (w *commitWaiter) checkCondition(session *partitionSession, offset rawtopicreader.Offset) (finished bool) {
	return session == w.Session && offset >= w.EndOffset
}

var commitWaiterLastID int64

func newCommitWaiter(session *partitionSession, endOffset rawtopicreader.Offset) commitWaiter {
	id := atomic.AddInt64(&commitWaiterLastID, 1)

	return commitWaiter{
		ID:        id,
		Session:   session,
		EndOffset: endOffset,
		Committed: make(empty.Chan, 1),
	}
}

func sendCommitMessage(send sendMessageToServerFunc, batch CommitRanges) error {
	req := &rawtopicreader.CommitOffsetRequest{
		CommitOffsets: batch.toPartitionsOffsets(),
	}

	return send(req)
}
