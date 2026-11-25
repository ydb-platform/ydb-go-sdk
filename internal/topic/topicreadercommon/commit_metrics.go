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
	tracker    *commitMessageTracker
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
		tracker:    &commitMessageTracker{session: s},
	}
}

func commitRangeMessageCount(start, end rawtopiccommon.Offset) int64 {
	if end <= start {
		return 0
	}

	return int64(end - start)
}

// RegisterCommitQueued records an accepted commit and returns its range span.
// Callers that need to synchronize admission with another lock must call this
// before making the commit visible to a sender.
func RegisterCommitQueued(commitRange CommitRange) int64 {
	session := commitRange.PartitionSession
	if session == nil || session.commitMetrics == nil {
		return 0
	}

	if session.ctx != nil && session.ctx.Err() != nil {
		return 0
	}

	return session.commitMetrics.tracker.Queue(commitRange.CommitOffsetStart, commitRange.CommitOffsetEnd)
}

// TraceCommitQueued records an accepted commit and emits its queued event.
// Call RegisterCommitQueued separately when registration must happen while a
// caller-owned admission lock is held.
func TraceCommitQueued(ctx context.Context, commitRange CommitRange) {
	messagesCount := RegisterCommitQueued(commitRange)
	traceCommitQueued(ctx, commitRange, messagesCount)
}

// TraceCommitQueuedAfterRegistration emits the lazy request's event outside its
// startOnce. The admission result is a flag, not a protocol offset span.
func TraceCommitQueuedAfterRegistration(ctx context.Context, commitRange CommitRange, registered int) {
	if registered != 0 {
		traceCommitQueued(ctx, commitRange, int64(commitRange.CommitOffsetEnd-commitRange.CommitOffsetStart))
	}
}

func traceCommitQueued(ctx context.Context, commitRange CommitRange, messagesCount int64) {
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
// returns only the newly completed range span. It publishes the new watermark
// before returning, so callers may now wake commit waiters.
func RegisterCommitAcknowledged(session *PartitionSession, exclusiveOffset rawtopiccommon.Offset) int64 {
	if session == nil {
		return 0
	}
	if session.commitMetrics == nil {
		session.SetCommittedOffsetForward(exclusiveOffset)

		return 0
	}

	return session.commitMetrics.tracker.Acknowledge(exclusiveOffset)
}

// CommitMessageTracker returns the optional tracker owned by the ACK receive path.
func CommitMessageTracker(session *PartitionSession) *commitMessageTracker {
	if session == nil || session.commitMetrics == nil {
		return nil
	}

	return session.commitMetrics.tracker
}

// commitMessageTracker tracks accepted commit ranges until the server
// acknowledges them.
//
// The acknowledgement offset is an exclusive upper bound. Each fully covered submission counts once.
// Queue and Acknowledge are safe to call
// concurrently. The tracker does not emit metrics; callers use their returned
// counts for that purpose.
type commitMessageTracker struct {
	mu sync.Mutex

	session *PartitionSession
	// pending is a binary min-heap ordered by end, so an acknowledgement
	// visits only the ranges it completes, whatever the submission order.
	pending []commitMetricRange
	closed  bool
}

type commitMetricRange struct {
	end   rawtopiccommon.Offset
	count int64
}

// Queue records a submitted commit range and returns its offset span. Ranges
// ending at or before the committed watermark are already complete; they
// need no new ACK and must not contribute to either counter.
func (t *commitMessageTracker) Queue(start, end rawtopiccommon.Offset) int64 {
	count := commitRangeMessageCount(start, end)
	if count == 0 {
		return 0
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed || end <= t.session.CommittedOffset() {
		return 0
	}

	t.pending = append(t.pending, commitMetricRange{end: end, count: count})
	for i := len(t.pending) - 1; i > 0; {
		parent := (i - 1) / 2
		if t.pending[parent].end <= t.pending[i].end {
			break
		}
		t.pending[parent], t.pending[i] = t.pending[i], t.pending[parent]
		i = parent
	}

	return count
}

// Acknowledge advances the exclusive committed offset and returns the sum of
// all pending ranges fully covered by it. Equal and backward
// acknowledgements cannot complete any new range.
func (t *commitMessageTracker) Acknowledge(exclusiveOffset rawtopiccommon.Offset) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	var acknowledged int64
	for len(t.pending) > 0 && t.pending[0].end <= exclusiveOffset {
		acknowledged += t.pending[0].count
		t.popPending()
	}

	t.session.SetCommittedOffsetForward(exclusiveOffset)

	return acknowledged
}

// popPending removes the pending range with the smallest end.
func (t *commitMessageTracker) popPending() {
	pending := t.pending
	last := len(pending) - 1
	pending[0] = pending[last]
	pending = pending[:last]
	for i := 0; ; {
		child := 2*i + 1
		if child+1 < last && pending[child+1].end < pending[child].end {
			child++
		}
		if child >= last || pending[i].end <= pending[child].end {
			break
		}
		pending[i], pending[child] = pending[child], pending[i]
		i = child
	}
	t.pending = pending
}

// Close marks the tracker terminal and releases all pending ranges.
func (t *commitMessageTracker) Close() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return
	}

	t.closed = true
	t.pending = nil
}
