package topicreadercommon

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type partitionSessionCommitMetrics struct {
	tracer     *trace.Topic
	readerInfo ReaderInfo
	tracker    *CommitMessageTracker
	closed     atomic.Bool
}

func (m *partitionSessionCommitMetrics) close() {
	if m == nil || !m.closed.CompareAndSwap(false, true) {
		return
	}

	if m.tracker != nil {
		m.tracker.Close()
	}
}

// SetupCommitMetrics enables commit range trace events and acknowledgement
// tracking for the partition session. It must be called before messages are
// delivered.
func (s *PartitionSession) SetupCommitMetrics(tracer *trace.Topic, readerInfo ReaderInfo) {
	if s == nil || tracer == nil {
		return
	}
	if s.ctx != nil && s.ctx.Err() != nil {
		return
	}
	if tracer.OnReaderCommitQueued == nil && tracer.OnReaderCommitAcknowledged == nil {
		return
	}
	if s.commitMetrics != nil {
		return
	}

	metrics := &partitionSessionCommitMetrics{
		tracer:     tracer,
		readerInfo: readerInfo,
	}
	if tracer.OnReaderCommitAcknowledged != nil {
		metrics.tracker = NewCommitMessageTracker(s.CommittedOffset())
	}
	s.commitMetrics = metrics
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
	if session == nil {
		return 0
	}
	if session.metricsSource != nil {
		session.metricsSource.RegisterCommit(session, commitRange.CommitOffsetEnd.ToInt64())
	}
	if session.commitMetrics == nil {
		return 0
	}

	metrics := session.commitMetrics
	if metrics.closed.Load() {
		return 0
	}
	messagesCount := commitRangeMessageCount(commitRange.CommitOffsetStart, commitRange.CommitOffsetEnd)
	if metrics.tracker != nil {
		messagesCount = metrics.tracker.Queue(commitRange.CommitOffsetStart, commitRange.CommitOffsetEnd)
	}

	return messagesCount
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
	if messagesCount <= 0 || metrics.tracer.OnReaderCommitQueued == nil {
		return
	}

	// The metric consumer only needs MessagesCount. Keep range boundaries
	// private to the tracker instead of allocating another representation for
	// every trace callback.
	gtrace.TopicOnReaderCommitQueued(
		metrics.tracer,
		&ctx,
		metrics.readerInfo.Endpoint,
		metrics.readerInfo.Database,
		session.Topic,
		metrics.readerInfo.Consumer,
		metrics.readerInfo.ReaderName,
		metrics.readerInfo.Listener,
		session.PartitionID,
		session.StreamPartitionSessionID.ToInt64(),
		messagesCount,
	)
}

// RegisterCommitAcknowledged records a successful commit acknowledgement and
// returns only the newly completed range span. The caller must perform this
// before publishing the committed offset or waking commit waiters.
func RegisterCommitAcknowledged(session *PartitionSession, exclusiveOffset rawtopiccommon.Offset) int {
	if session == nil {
		return 0
	}
	if session.commitMetrics == nil || session.commitMetrics.tracker == nil {
		return 0
	}

	metrics := session.commitMetrics
	if metrics.closed.Load() {
		return 0
	}

	return metrics.tracker.Acknowledge(exclusiveOffset)
}

// TraceCommitAcknowledgedAfterRegistration emits a successful commit
// acknowledgement after the caller has registered it with
// RegisterCommitAcknowledged. The callback runs even if the session closes
// after registration.
func TraceCommitAcknowledgedAfterRegistration(
	ctx context.Context,
	session *PartitionSession,
	messagesCount int,
) {
	if session == nil || session.commitMetrics == nil || messagesCount == 0 {
		return
	}

	metrics := session.commitMetrics
	if metrics.tracer.OnReaderCommitAcknowledged == nil {
		return
	}

	gtrace.TopicOnReaderCommitAcknowledged(
		metrics.tracer,
		&ctx,
		metrics.readerInfo.Endpoint,
		metrics.readerInfo.Database,
		session.Topic,
		metrics.readerInfo.Consumer,
		metrics.readerInfo.ReaderName,
		metrics.readerInfo.Listener,
		session.PartitionID,
		session.StreamPartitionSessionID.ToInt64(),
		messagesCount,
	)
}

// TraceCommitAcknowledged records a successful commit acknowledgement and
// emits only the newly completed range span.
func TraceCommitAcknowledged(ctx context.Context, session *PartitionSession, exclusiveOffset rawtopiccommon.Offset) {
	messagesCount := RegisterCommitAcknowledged(session, exclusiveOffset)
	TraceCommitAcknowledgedAfterRegistration(ctx, session, messagesCount)
}

// CommitMessageTracker tracks accepted commit ranges until the server
// acknowledges them.
//
// The acknowledgement offset is an exclusive upper bound. Ranges are kept in
// admission order so an acknowledgement only completes a prefix, matching
// the commit request semantics. Queue and Acknowledge are safe to call
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
// ending before the committed watermark are already complete and are not
// retained. A range ending exactly at the watermark is retained so an equal
// acknowledgement can complete it.
func (t *CommitMessageTracker) Queue(start, end rawtopiccommon.Offset) int {
	count := commitRangeMessageCount(start, end)
	if count == 0 {
		return 0
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return 0
	}
	if end < t.committedOffset {
		return count
	}

	t.pending = append(t.pending, commitMetricRange{end: end, count: count})

	return count
}

// Acknowledge advances the exclusive committed offset and returns the sum of
// the ranges at the head of the queue newly covered by it. Backward
// acknowledgements return zero; an equal acknowledgement can complete ranges
// admitted at the current watermark.
func (t *CommitMessageTracker) Acknowledge(exclusiveOffset rawtopiccommon.Offset) int {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed || exclusiveOffset < t.committedOffset {
		return 0
	}

	if exclusiveOffset > t.committedOffset {
		t.committedOffset = exclusiveOffset
	}

	var acknowledged int
	completed := 0
	for completed < len(t.pending) && t.pending[completed].end <= t.committedOffset {
		acknowledged += t.pending[completed].count
		completed++
	}

	if completed > 0 {
		t.pending = t.pending[completed:]
		if len(t.pending) == 0 {
			t.pending = nil
		}
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
