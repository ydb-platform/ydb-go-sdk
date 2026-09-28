package topicreadercommon

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

// commitRequest coordinates a single commit send across concurrent callers.
// It starts lazily, and Wait callers share its send result.
type commitRequest struct {
	committer   *Committer
	commitRange CommitRange
	startOnce   sync.Once
	sendDone    empty.Chan
	sendErr     error
}

// NewCommitRequest creates a lazy request for one commit range.
// Confirm or Wait can start the send; creating the request does not enqueue it.
func (c *Committer) NewCommitRequest(commitRange CommitRange) *commitRequest {
	return &commitRequest{
		committer:   c,
		commitRange: commitRange,
		sendDone:    make(empty.Chan),
	}
}

func (r *commitRequest) start() {
	r.startOnce.Do(func() {
		r.committer.pushRequest(r)
	})
}

// Confirm queues the commit if needed and returns without waiting for its send
// or ACK. Use Wait to observe either error.
func (r *commitRequest) Confirm() {
	r.start()
}

// Wait starts the commit if needed, then waits for the send result and ACK.
// Canceling ctx stops only this wait: another caller can keep waiting without
// sending the commit again. An already committed range is not sent again.
// A missing partition session is treated as expired because there is no live
// session to commit to.
func (r *commitRequest) Wait(ctx context.Context) error {
	if !r.committer.mode.CommitsEnabled() {
		return xerrors.WithStackTrace(ErrCommitDisabled)
	}
	session := r.commitRange.PartitionSession
	if session == nil {
		return xerrors.WithStackTrace(ErrPublicCommitSessionToExpiredSession)
	}
	if err := ctx.Err(); err != nil {
		return xerrors.WithStackTrace(err)
	}
	select {
	case <-r.sendDone:
		if r.sendErr != nil {
			return r.sendErr
		}
	default:
	}
	if session.CommittedOffset() >= r.commitRange.CommitOffsetEnd {
		r.startOnce.Do(func() {
			r.finishSend(nil)
		})

		return nil
	}
	if session.Context().Err() != nil {
		return xerrors.WithStackTrace(ErrPublicCommitSessionToExpiredSession)
	}
	r.start()
	if err := r.waitSend(ctx, session); err != nil {
		return err
	}

	return r.waitAck(ctx, session)
}

func (r *commitRequest) waitSend(ctx context.Context, session *PartitionSession) error {
	select {
	case <-r.sendDone:
		return r.sendErr
	case <-ctx.Done():
		return xerrors.WithStackTrace(ctx.Err())
	case <-session.Context().Done():
		if err := ctx.Err(); err != nil {
			return xerrors.WithStackTrace(err)
		}
		select {
		case <-r.sendDone:
			if r.sendErr != nil {
				return r.sendErr
			}
		default:
		}
		if session.CommittedOffset() >= r.commitRange.CommitOffsetEnd {
			return nil
		}

		return xerrors.WithStackTrace(ErrPublicCommitSessionToExpiredSession)
	}
}

func (r *commitRequest) waitAck(ctx context.Context, session *PartitionSession) error {
	if err := ctx.Err(); err != nil {
		return xerrors.WithStackTrace(err)
	}

	waiter := newCommitWaiter(session, r.commitRange.CommitOffsetEnd)
	var acknowledged bool
	r.committer.m.WithLock(func() {
		acknowledged = session.CommittedOffset() >= r.commitRange.CommitOffsetEnd
		if !acknowledged {
			r.committer.addWaiterNeedLock(waiter)
		}
	})
	if acknowledged {
		return nil
	}

	return r.committer.waitCommitAck(ctx, waiter)
}

func (r *commitRequest) finishSend(err error) {
	r.sendErr = err
	close(r.sendDone)
}
