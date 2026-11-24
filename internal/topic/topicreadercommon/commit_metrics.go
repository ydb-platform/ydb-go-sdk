package topicreadercommon

import (
	"context"
	"math/bits"
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
// returns only the newly completed range span. It publishes the new watermark
// before returning, so callers may now wake commit waiters.
func RegisterCommitAcknowledged(session *PartitionSession, exclusiveOffset rawtopiccommon.Offset) int {
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
	pending *commitMetricRange
	closed  bool
}

const (
	radixBits = 4
	radixSize = 1 << radixBits
)

type commitMetricRange struct {
	key   uint64 // representative key for the shared high bits
	shift int    // bit position of this radix digit
	sum   int
	count [radixSize]int
	child [radixSize]*commitMetricRange
}

// Each level indexes one hex digit of the exclusive end. Adjacent commits
// share a leaf; sparse offsets branch only at digits where their keys differ.
func (r *commitMetricRange) insert(key uint64, count int) *commitMetricRange {
	if r == nil {
		r = &commitMetricRange{key: key}
	} else if width := bits.Len64(key ^ r.key); width > r.shift+radixBits {
		parent := &commitMetricRange{key: key, shift: (width - 1) / radixBits * radixBits, sum: r.sum}
		i := (r.key >> parent.shift) % radixSize
		parent.child[i], parent.count[i] = r, r.sum
		r = parent
	}
	i := (key >> r.shift) % radixSize
	if r.shift != 0 {
		r.child[i] = r.child[i].insert(key, count)
	}
	r.count[i] += count
	r.sum += count

	return r
}

// Drop covered slots as units. Only the boundary slot can be partly covered.
func (r *commitMetricRange) acknowledge(key uint64) (*commitMetricRange, int) {
	if r == nil {
		return nil, 0
	}
	switch prefix, boundary := r.key>>(r.shift+radixBits), key>>(r.shift+radixBits); {
	case prefix < boundary:
		return nil, r.sum
	case prefix > boundary:
		return r, 0
	}
	i := (key >> r.shift) % radixSize
	covered := 0
	for j := range i {
		covered += r.count[j]
		r.count[j], r.child[j] = 0, nil
	}
	count := r.count[i]
	if r.shift != 0 {
		r.child[i], count = r.child[i].acknowledge(key)
	}
	r.count[i] -= count
	covered += count
	r.sum -= covered
	if r.sum == 0 {
		return nil, covered
	}

	return r, covered
}

// Queue records a submitted commit range and returns its offset span. Ranges
// ending at or before the committed watermark are already complete; they
// need no new ACK and must not contribute to either counter.
func (t *commitMessageTracker) Queue(start, end rawtopiccommon.Offset) int {
	count := commitRangeMessageCount(start, end)
	if count == 0 {
		return 0
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed || end <= t.session.CommittedOffset() {
		return 0
	}

	t.pending = t.pending.insert(uint64(end)^(1<<63), count)

	return count
}

// Acknowledge advances the exclusive committed offset and returns the sum of
// all pending ranges fully covered by it. Equal and backward
// acknowledgements cannot complete any new range.
func (t *commitMessageTracker) Acknowledge(exclusiveOffset rawtopiccommon.Offset) int {
	t.mu.Lock()
	defer t.mu.Unlock()

	var acknowledged int
	t.pending, acknowledged = t.pending.acknowledge(uint64(exclusiveOffset) ^ (1 << 63))

	t.session.SetCommittedOffsetForward(exclusiveOffset)

	return acknowledged
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
