package topicreadercommon

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type commitMessageMetadata struct {
	start  rawtopiccommon.Offset
	end    rawtopiccommon.Offset
	offset rawtopiccommon.Offset
}

func mergeCommitMessageMetadata(lhs, rhs []commitMessageMetadata) []commitMessageMetadata {
	if lhs == nil || rhs == nil {
		return nil
	}

	return appendCommitMessageMetadata(lhs, rhs, len(lhs), len(rhs))
}

func appendCommitMessageMetadata(
	lhs []commitMessageMetadata,
	rhs []commitMessageMetadata,
	lhsMessagesCount int,
	rhsMessagesCount int,
) []commitMessageMetadata {
	if lhsMessagesCount == 0 {
		return rhs
	}
	if rhsMessagesCount == 0 {
		return lhs
	}

	return append(slices.Grow(lhs, len(rhs)), rhs...)
}

func singleCommitMessageMetadata(start, end, offset rawtopiccommon.Offset) []commitMessageMetadata {
	return []commitMessageMetadata{{
		start:  start,
		end:    end,
		offset: offset,
	}}
}

func commitMessageMetadataForMessage(message *PublicMessage) ([]commitMessageMetadata, bool) {
	if message.commitRange.messageMetadata != nil {
		if len(message.commitRange.messageMetadata) != 1 {
			return nil, false
		}

		return message.commitRange.messageMetadata, true
	}

	if message.commitRange.CommitOffsetEnd != message.commitRange.CommitOffsetStart+1 {
		return nil, false
	}

	return singleCommitMessageMetadata(
		message.commitRange.CommitOffsetStart,
		message.commitRange.CommitOffsetEnd,
		message.commitRange.CommitOffsetStart,
	), true
}

func commitMessageMetadataComplete(commitRange CommitRange, messagesCount int) bool {
	return len(commitRange.messageMetadata) == messagesCount
}

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

// SetupCommitMetrics enables commit identity tracking and commit trace events
// for the partition session. It must be called before messages are delivered.
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

func (s *PartitionSession) commitMetricsEnabled() bool {
	return s != nil && s.commitMetrics != nil
}

// MessageOffsets returns a copy of the immutable logical message offsets
// captured for this range. It returns nil when the range was created without
// commit identity tracking or when the range contains no messages.
func (c CommitRange) MessageOffsets() []rawtopiccommon.Offset {
	if len(c.messageMetadata) == 0 {
		return nil
	}

	res := make([]rawtopiccommon.Offset, len(c.messageMetadata))
	for i := range c.messageMetadata {
		res[i] = c.messageMetadata[i].offset
	}

	return res
}

// RegisterCommitQueued records an accepted commit and returns its logical
// message count. Callers that need to synchronize admission with another
// lock must call this before making the commit visible to a sender.
func RegisterCommitQueued(commitRange CommitRange) int {
	session := commitRange.PartitionSession
	if session == nil || session.commitMetrics == nil {
		return 0
	}

	metrics := session.commitMetrics
	if metrics.closed.Load() {
		return 0
	}
	offsets := commitRange.MessageOffsets()
	messagesCount := len(offsets)
	if metrics.tracker != nil {
		messagesCount = metrics.tracker.Queue(offsets)
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

	// The metric consumer only needs MessagesCount. Keep the exact offsets
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
		session.PartitionID,
		session.StreamPartitionSessionID.ToInt64(),
		messagesCount,
	)
}

// RegisterCommitAcknowledged records a successful commit acknowledgement and
// returns only the newly acknowledged logical message count. The caller must
// perform this before publishing the committed offset or waking commit
// waiters.
func RegisterCommitAcknowledged(session *PartitionSession, exclusiveOffset rawtopiccommon.Offset) int {
	if session == nil || session.commitMetrics == nil || session.commitMetrics.tracker == nil {
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
		session.PartitionID,
		session.StreamPartitionSessionID.ToInt64(),
		messagesCount,
	)
}

// TraceCommitAcknowledged records a successful commit acknowledgement and
// emits only the newly acknowledged logical messages.
func TraceCommitAcknowledged(ctx context.Context, session *PartitionSession, exclusiveOffset rawtopiccommon.Offset) {
	messagesCount := RegisterCommitAcknowledged(session, exclusiveOffset)
	TraceCommitAcknowledgedAfterRegistration(ctx, session, messagesCount)
}

// CommitMessageTracker tracks the logical message offsets admitted to a
// partition session until the server acknowledges them.
//
// The acknowledgement offset is an exclusive upper bound: an offset is
// acknowledged when it is less than the supplied value. Queue and
// Acknowledge are safe to call concurrently. The tracker does not emit
// metrics; callers use their returned counts for that purpose.
type CommitMessageTracker struct {
	mu sync.Mutex

	committedOffset rawtopiccommon.Offset
	pending         map[rawtopiccommon.Offset]struct{}
	closed          bool
}

// NewCommitMessageTracker creates a tracker with the initial exclusive
// committed offset.
func NewCommitMessageTracker(committedOffset rawtopiccommon.Offset) *CommitMessageTracker {
	return &CommitMessageTracker{
		committedOffset: committedOffset,
	}
}

// Queue records submitted message offsets and returns the number of submitted
// offsets. Repeated offsets are counted in the return value but retained only
// once for acknowledgement accounting. Offsets below the committed watermark
// are accepted but are not retained.
func (t *CommitMessageTracker) Queue(offsets []rawtopiccommon.Offset) int {
	if len(offsets) == 0 {
		return 0
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return 0
	}

	for _, offset := range offsets {
		if offset < t.committedOffset {
			continue
		}

		if t.pending == nil {
			t.pending = make(map[rawtopiccommon.Offset]struct{})
		}
		t.pending[offset] = struct{}{}
	}

	return len(offsets)
}

// Acknowledge advances the exclusive committed offset and returns the number
// of unique queued messages newly covered by it. Backward and duplicate
// acknowledgements return zero.
func (t *CommitMessageTracker) Acknowledge(exclusiveOffset rawtopiccommon.Offset) int {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed || exclusiveOffset <= t.committedOffset {
		return 0
	}

	t.committedOffset = exclusiveOffset

	var acknowledged int
	for offset := range t.pending {
		if offset >= exclusiveOffset {
			continue
		}

		delete(t.pending, offset)
		acknowledged++
	}

	if len(t.pending) == 0 {
		t.pending = nil
	}

	return acknowledged
}

// Close marks the tracker terminal and releases all pending offsets.
func (t *CommitMessageTracker) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return
	}

	t.closed = true
	t.pending = nil
}
