package topicreadercommon

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
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

func (r *commitRequest) start() (started bool) {
	r.startOnce.Do(func() {
		started = true
		r.committer.pushRequest(r)
	})

	return started
}

// Confirm starts the commit if needed. The caller that starts it waits for the
// send to finish; other callers do not wait. Confirm neither waits for an ACK
// nor reports a send error.
func (r *commitRequest) Confirm() {
	if r.start() {
		<-r.sendDone
	}
}

// Wait starts the commit if needed, then waits for the send result and ACK.
// Canceling ctx stops only this wait: another caller can keep waiting without
// sending the commit again. An already committed range is not sent again.
// Waiting for an ACK requires sync commit mode.
//
//nolint:funlen // Keep the send and ACK phases together to preserve their error order.
func (r *commitRequest) Wait(ctx context.Context) error {
	if r.committer.mode != CommitModeSync {
		return ErrWaitAckRequiresSyncMode
	}
	session := r.commitRange.PartitionSession
	if session == nil {
		return ErrPublicCommitSessionToExpiredSession
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-r.sendDone:
		if r.sendErr != nil {
			return r.sendErr
		}
	default:
	}
	if session.CommittedOffset() >= r.commitRange.CommitOffsetEnd {
		skipped := false
		r.startOnce.Do(func() {
			skipped = true
			r.finishSend(nil)
		})
		if skipped {
			return nil
		}
	}
	if session.Context().Err() != nil {
		return ErrPublicCommitSessionToExpiredSession
	}
	r.start()
	select {
	case <-r.sendDone:
		if r.sendErr != nil {
			return r.sendErr
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-session.Context().Done():
		return ErrPublicCommitSessionToExpiredSession
	}

	if session.Context().Err() != nil {
		return ErrPublicCommitSessionToExpiredSession
	}
	if err := ctx.Err(); err != nil {
		return err
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
