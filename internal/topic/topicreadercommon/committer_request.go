package topicreadercommon

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
)

type commitRequest struct {
	committer   *Committer
	commitRange CommitRange
	startOnce   sync.Once
	sendDone    empty.Chan
	sendErr     error
}

// NewCommitRequest prepares a commit range for a single send on Confirm or Wait.
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

func (r *commitRequest) Confirm() {
	if r.start() {
		<-r.sendDone
	}
}

func (r *commitRequest) Wait(ctx context.Context) error {
	if err := r.commitRange.PartitionSession.Context().Err(); err != nil {
		return ErrPublicCommitSessionToExpiredSession
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.commitRange.PartitionSession.CommittedOffset() >= r.commitRange.CommitOffsetEnd {
		skipped := false
		r.startOnce.Do(func() {
			skipped = true
			r.finishSend(nil)
		})
		if skipped {
			return nil
		}
	}
	r.start()
	select {
	case <-r.sendDone:
		if r.sendErr != nil {
			return r.sendErr
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-r.commitRange.PartitionSession.Context().Done():
		return ErrPublicCommitSessionToExpiredSession
	}

	return r.committer.WaitAck(ctx, r.commitRange)
}

func (r *commitRequest) finishSend(err error) {
	if err != nil && r.committer.backgroundWorker.Context().Err() != nil {
		err = ErrPublicCommitSessionToExpiredSession
	}
	r.sendErr = err
	close(r.sendDone)
}
