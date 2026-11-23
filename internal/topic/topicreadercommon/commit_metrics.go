package topicreadercommon

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type partitionSessionCommitMetrics struct {
	tracer     *trace.Topic
	readerInfo ReaderInfo
	tracker    *CommitMessageTracker
}

// SetupCommitMetrics enables commit range trace events and acknowledgement
// tracking for the partition session when queued-commit tracing is enabled. It
// must be called before messages are delivered.
func (s *PartitionSession) SetupCommitMetrics(tracer *trace.Topic, readerInfo ReaderInfo) {
	if s == nil || tracer == nil {
		return
	}
	if s.ctx != nil && s.ctx.Err() != nil {
		return
	}
	if tracer.OnReaderCommitQueued == nil {
		return
	}
	if s.commitMetrics != nil {
		return
	}

	s.commitMetrics = &partitionSessionCommitMetrics{
		tracer:     tracer,
		readerInfo: readerInfo,
		tracker:    NewCommitMessageTracker(s.CommittedOffset()),
	}
}

func commitRangeMessageCount(start, end rawtopiccommon.Offset) int {
	if end <= start {
		return 0
	}

	return int(end - start)
}

// RegisterCommitQueued records an accepted commit and returns its range span.
// Callers that need to synchronize admission with another lock must call this
// before making the commit visible to a sender.
func RegisterCommitQueued(commitRange CommitRange) int {
	session := commitRange.PartitionSession
	if session == nil || session.commitMetrics == nil {
		return 0
	}

	return session.commitMetrics.tracker.Queue(commitRange.CommitOffsetStart, commitRange.CommitOffsetEnd)
}

// TraceCommitQueued records an accepted commit and emits its queued event.
// Call RegisterCommitQueued separately when registration must happen while a
// caller-owned admission lock is held.
func TraceCommitQueued(ctx context.Context, commitRange CommitRange) {
	messagesCount := RegisterCommitQueued(commitRange)
	TraceCommitQueuedAfterRegistration(ctx, commitRange, messagesCount)
}

// TraceCommitQueuedAfterRegistration emits the queued event after the caller
// has registered the commit with RegisterCommitQueued. The callback runs
// outside the caller's admission lock.
func TraceCommitQueuedAfterRegistration(ctx context.Context, commitRange CommitRange, messagesCount int) {
	session := commitRange.PartitionSession
	if session == nil || session.commitMetrics == nil {
		return
	}

	metrics := session.commitMetrics
	if messagesCount <= 0 {
		return
	}

	gtrace.TopicOnReaderCommitQueued(
		metrics.tracer,
		&ctx,
		metrics.readerInfo,
		session.Topic,
		session.PartitionID,
		session.StreamPartitionSessionID.ToInt64(),
		messagesCount,
	)
}

// RegisterCommitAcknowledged records a successful commit acknowledgement and
// returns only the newly completed range span. The caller must perform this
// before publishing the committed offset or waking commit waiters.
func RegisterCommitAcknowledged(session *PartitionSession, exclusiveOffset rawtopiccommon.Offset) int {
	if session == nil || session.commitMetrics == nil {
		return 0
	}

	return session.commitMetrics.tracker.Acknowledge(exclusiveOffset)
}

// CommitMessageTracker tracks accepted commit ranges until the server
// acknowledges them.
//
// The acknowledgement offset is an exclusive upper bound. Each fully covered submission counts once.
// Queue and Acknowledge are safe to call
// concurrently. The tracker does not emit metrics; callers use their returned
// counts for that purpose.
type CommitMessageTracker struct {
	mu sync.Mutex

	committedOffset rawtopiccommon.Offset
	pending         []commitMetricRange
	closed          bool
}

type commitMetricRange struct {
	end   rawtopiccommon.Offset
	count int
}

// NewCommitMessageTracker creates a tracker with the initial exclusive
// committed offset.
func NewCommitMessageTracker(committedOffset rawtopiccommon.Offset) *CommitMessageTracker {
	return &CommitMessageTracker{
		committedOffset: committedOffset,
	}
}

// Queue records a submitted commit range and returns its offset span. Ranges
// ending at or before the committed watermark are already complete; they
// need no new ACK and must not contribute to either counter.
func (t *CommitMessageTracker) Queue(start, end rawtopiccommon.Offset) int {
	count := commitRangeMessageCount(start, end)
	if count == 0 {
		return 0
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed || end <= t.committedOffset {
		return 0
	}

	t.pending = append(t.pending, commitMetricRange{end: end, count: count})

	return count
}

// Acknowledge advances the exclusive committed offset and returns the sum of
// all pending ranges fully covered by it. Equal and backward
// acknowledgements cannot complete any new range.
func (t *CommitMessageTracker) Acknowledge(exclusiveOffset rawtopiccommon.Offset) int {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed || exclusiveOffset <= t.committedOffset {
		return 0
	}

	t.committedOffset = exclusiveOffset
	var acknowledged int
	pending := t.pending[:0]
	for _, r := range t.pending {
		if r.end <= exclusiveOffset {
			acknowledged += r.count
		} else {
			pending = append(pending, r)
		}
	}
	t.pending = pending
	if len(pending) == 0 {
		t.pending = nil
	}

	return acknowledged
}

// Close marks the tracker terminal and releases all pending ranges.
func (t *CommitMessageTracker) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return
	}

	t.closed = true
	t.pending = nil
}
