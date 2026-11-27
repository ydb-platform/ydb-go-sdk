package topicreadercommon

import (
	"context"
	"math/rand/v2"
	"sort"
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
// tracking for the partition session when queued-commit tracing is enabled for
// its reader kind. It must be called before messages are delivered.
func (s *PartitionSession) SetupCommitMetrics(tracer *trace.Topic, readerInfo ReaderInfo) {
	if s == nil || tracer == nil {
		return
	}
	if s.ctx != nil && s.ctx.Err() != nil {
		return
	}
	queued := tracer.OnReaderCommitQueued
	if readerInfo.Listener {
		queued = tracer.OnListenerCommitQueued
	}
	if queued == nil {
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

func traceCommitQueued(ctx context.Context, commitRange CommitRange, messagesCount int64) {
	session := commitRange.PartitionSession
	if session == nil || session.commitMetrics == nil {
		return
	}

	metrics := session.commitMetrics
	if messagesCount <= 0 {
		return
	}

	emit := gtrace.TopicOnReaderCommitQueued
	if metrics.readerInfo.Listener {
		emit = gtrace.TopicOnListenerCommitQueued
	}
	emit(
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

// commitMessageTracker tracks accepted commit ranges until the server
// acknowledges them.
//
// The acknowledgement offset is an exclusive upper bound. Each fully covered submission counts once.
// Queue and Acknowledge are safe to call
// concurrently. The tracker does not emit metrics; callers use their returned
// counts for that purpose. An acknowledgement can still arrive after the
// session context is canceled. A full acknowledgement releases all storage.
type commitMessageTracker struct {
	mu sync.Mutex

	session *PartitionSession
	owner   *pendingCommitTrackers
	// ordered stores non-decreasing ends and their cumulative counts.
	ordered    []commitMetricRange
	prefixBase int64 // cumulative count before ordered[0]
	total      int64
	// pending indexes ends below the last ordered end.
	pending *commitRangeNode
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

	if end <= t.session.CommittedOffset() {
		return 0
	}
	if len(t.ordered) == 0 && t.owner != nil && !t.owner.add(t) {
		return 0
	}

	t.total += count
	if len(t.ordered) == 0 {
		t.ordered = append(t.ordered, commitMetricRange{end: end, count: count})
	} else if last := &t.ordered[len(t.ordered)-1]; end == last.end {
		last.count += count
	} else if end > last.end {
		t.ordered = append(t.ordered, commitMetricRange{end: end, count: last.count + count})
	} else {
		// Priorities must be unpredictable: a known sequence lets an admission
		// order degrade the treap into a list. The runtime source takes no lock.
		t.pending = t.pending.insert(commitMetricRange{end: end, count: count}, rand.Uint64())
	}

	return count
}

// Acknowledge advances the committed watermark and counts pending ordinary
// ranges newly covered by this stream ACK. A transaction may already have
// advanced the watermark without completing those ranges.
func (t *commitMessageTracker) Acknowledge(exclusiveOffset rawtopiccommon.Offset) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.session.SetCommittedOffsetForward(exclusiveOffset)
	if len(t.ordered) == 0 {
		return 0
	}
	// The last ordered end is also the maximum across both structures.
	if exclusiveOffset >= t.ordered[len(t.ordered)-1].end {
		acknowledged := t.total
		t.clear()
		if t.owner != nil {
			t.owner.remove(t)
		}

		return acknowledged
	}

	var acknowledged int64
	if exclusiveOffset >= t.ordered[0].end {
		firstUncovered := sort.Search(len(t.ordered), func(i int) bool {
			return t.ordered[i].end > exclusiveOffset
		})
		prefix := t.ordered[firstUncovered-1].count
		acknowledged = prefix - t.prefixBase
		t.prefixBase = prefix
		t.ordered = t.ordered[firstUncovered:]
	}
	var covered int64
	t.pending, covered = t.pending.removeThrough(exclusiveOffset)
	acknowledged += covered
	t.total -= acknowledged

	return acknowledged
}

func (t *commitMessageTracker) discard() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.clear()
}

func (t *commitMessageTracker) clear() {
	t.ordered, t.pending = nil, nil
	t.prefixBase, t.total = 0, 0
}
