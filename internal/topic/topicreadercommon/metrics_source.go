package topicreadercommon

import (
	"container/heap"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// ReaderMetricsSource keeps the state needed by observable topic reader
// metrics for one logical reader or listener lifetime.
//
// The source is created only when the corresponding optional trace hook is
// installed. Message timestamps are kept by message identity so batches can
// be merged or split without changing the age of retained messages.
type ReaderMetricsSource struct {
	mu sync.Mutex

	closed bool

	messageReceivedAt map[*PublicMessage]time.Time
	receiptCounts     map[time.Time]int
	receiptEntries    map[time.Time]*receiptTime
	receiptTimes      receiptTimeHeap
	oldestMessageAt   time.Time
	partitions        map[*PartitionSession]readerMetricsPartition
}

type readerMetricsPartition struct {
	requestedCommitEnd    int64
	acknowledgedCommitEnd int64
}

type receiptTime struct {
	at    time.Time
	index int
}

type receiptTimeHeap []*receiptTime

func (h receiptTimeHeap) Len() int { return len(h) }

func (h receiptTimeHeap) Less(i, j int) bool { return h[i].at.Before(h[j].at) }

func (h receiptTimeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *receiptTimeHeap) Push(value any) {
	entry, ok := value.(*receiptTime)
	if !ok {
		return
	}
	entry.index = len(*h)
	*h = append(*h, entry)
}

func (h *receiptTimeHeap) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	value.index = -1
	old[last] = nil
	*h = old[:last]

	return value
}

// NewReaderMetricsSource creates an empty source for one logical reader or
// listener lifetime.
func NewReaderMetricsSource() *ReaderMetricsSource {
	return &ReaderMetricsSource{
		messageReceivedAt: make(map[*PublicMessage]time.Time),
		receiptCounts:     make(map[time.Time]int),
		receiptEntries:    make(map[time.Time]*receiptTime),
		partitions:        make(map[*PartitionSession]readerMetricsPartition),
	}
}

// Snapshot returns a point-in-time view of the source without invoking any
// observer or registry callback while the source lock is held.
func (s *ReaderMetricsSource) Snapshot() trace.TopicReaderMetricsSnapshot {
	if s == nil {
		return trace.TopicReaderMetricsSnapshot{}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.advanceOldestLocked()

	var oldestMessageAge time.Duration
	if !s.oldestMessageAt.IsZero() {
		oldestMessageAge = time.Since(s.oldestMessageAt)
		oldestMessageAge = max(oldestMessageAge, 0)
	}

	var commitOffsetLag int64
	for partition := range s.partitions {
		state := s.partitions[partition]
		lag := state.requestedCommitEnd - state.acknowledgedCommitEnd
		if lag > commitOffsetLag {
			commitOffsetLag = lag
		}
	}

	return trace.TopicReaderMetricsSnapshot{
		OldestMessageAge:      oldestMessageAge,
		CommitOffsetLag:       commitOffsetLag,
		PartitionSessionCount: int64(len(s.partitions)),
	}
}

// TrackBatch records messages retained by the SDK. receivedAt must be taken
// when the response is received, before any lazy message decoding occurs.
func (s *ReaderMetricsSource) TrackBatch(batch *PublicBatch, receivedAt time.Time) {
	if s == nil || batch == nil || len(batch.Messages) == 0 {
		return
	}
	if receivedAt.IsZero() {
		receivedAt = time.Now()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}

	for _, message := range batch.Messages {
		if message == nil {
			continue
		}
		if _, exists := s.messageReceivedAt[message]; exists {
			continue
		}
		s.messageReceivedAt[message] = receivedAt
		if s.receiptCounts[receivedAt] == 0 {
			entry := &receiptTime{at: receivedAt}
			s.receiptEntries[receivedAt] = entry
			heap.Push(&s.receiptTimes, entry)
		}
		s.receiptCounts[receivedAt]++
	}
	s.advanceOldestLocked()
}

// ReleaseBatch removes messages that are no longer retained by the SDK.
func (s *ReaderMetricsSource) ReleaseBatch(batch *PublicBatch) {
	if s == nil || batch == nil || len(batch.Messages) == 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}

	for _, message := range batch.Messages {
		receivedAt, ok := s.messageReceivedAt[message]
		if !ok {
			continue
		}
		delete(s.messageReceivedAt, message)
		if count := s.receiptCounts[receivedAt]; count <= 1 {
			delete(s.receiptCounts, receivedAt)
			if entry := s.receiptEntries[receivedAt]; entry != nil {
				delete(s.receiptEntries, receivedAt)
				heap.Remove(&s.receiptTimes, entry.index)
			}
		} else {
			s.receiptCounts[receivedAt] = count - 1
		}
	}
	s.advanceOldestLocked()
}

func (s *ReaderMetricsSource) advanceOldestLocked() {
	for len(s.receiptTimes) > 0 {
		oldest := s.receiptTimes[0].at
		if s.receiptCounts[oldest] > 0 {
			s.oldestMessageAt = oldest

			return
		}
		entry, ok := heap.Pop(&s.receiptTimes).(*receiptTime)
		if !ok {
			continue
		}
		delete(s.receiptEntries, entry.at)
	}
	s.oldestMessageAt = time.Time{}
}

// RegisterPartitionSession starts tracking one active partition session.
func (s *ReaderMetricsSource) RegisterPartitionSession(session *PartitionSession) {
	if s == nil || session == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	if session.ctx != nil && session.ctx.Err() != nil {
		return
	}
	if _, exists := s.partitions[session]; exists {
		return
	}
	committedOffset := session.CommittedOffset().ToInt64()
	s.partitions[session] = readerMetricsPartition{
		requestedCommitEnd:    committedOffset,
		acknowledgedCommitEnd: committedOffset,
	}
}

// SetInitialCommittedOffset updates the baseline used by commit lag after a
// start-session callback changes the initial offset.
func (s *ReaderMetricsSource) SetInitialCommittedOffset(session *PartitionSession, committedOffset int64) {
	if s == nil || session == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	state, exists := s.partitions[session]
	if !exists {
		return
	}
	state.requestedCommitEnd = committedOffset
	state.acknowledgedCommitEnd = committedOffset
	s.partitions[session] = state
}

// UnregisterPartitionSession stops tracking one active partition session.
func (s *ReaderMetricsSource) UnregisterPartitionSession(session *PartitionSession) {
	if s == nil || session == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.partitions, session)
}

// RegisterCommit records the greatest non-transactional commit end requested
// for an active partition session.
func (s *ReaderMetricsSource) RegisterCommit(session *PartitionSession, commitEnd int64) {
	if s == nil || session == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	state, exists := s.partitions[session]
	if !exists || commitEnd <= state.requestedCommitEnd {
		return
	}
	state.requestedCommitEnd = commitEnd
	s.partitions[session] = state
}

// AcknowledgeCommit records the greatest acknowledged committed offset for an
// active partition session.
func (s *ReaderMetricsSource) AcknowledgeCommit(session *PartitionSession, committedOffset int64) {
	if s == nil || session == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	state, exists := s.partitions[session]
	if !exists || committedOffset <= state.acknowledgedCommitEnd {
		return
	}
	state.acknowledgedCommitEnd = committedOffset
	s.partitions[session] = state
}

// Close releases all source state. It is safe to call more than once.
func (s *ReaderMetricsSource) Close() {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	s.messageReceivedAt = nil
	s.receiptCounts = nil
	s.receiptEntries = nil
	s.receiptTimes = nil
	s.oldestMessageAt = time.Time{}
	s.partitions = nil
}
