package topicreadercommon

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func BenchmarkCommitCumulativeAcknowledgement(b *testing.B) {
	for _, n := range []rawtopiccommon.Offset{1, 100_000, 500_000} {
		for _, ack := range []rawtopiccommon.Offset{n, n / 2} {
			b.Run(fmt.Sprintf("pending=%d/ack=%d", n, ack), func(b *testing.B) {
				b.StopTimer()
				tracer := &trace.Topic{OnReaderCommitQueued: func(trace.TopicReaderCommitQueuedInfo) {}}
				b.ReportAllocs()
				for range b.N {
					session := NewPartitionSession(b.Context(), "topic", 1, 1, "stream", 1, 1, 0)
					session.SetupCommitMetrics(tracer, ReaderInfo{})
					for i := range n {
						RegisterCommitQueued(CommitRange{
							PartitionSession: session, CommitOffsetStart: i, CommitOffsetEnd: i + 1,
						})
					}
					b.StartTimer()
					got := RegisterCommitAcknowledged(session, ack)
					b.StopTimer()
					require.Equal(b, int64(ack), got)
					session.Close()
				}
			})
		}
	}
}

func BenchmarkCommitMessageTrackerIncrementalAck(b *testing.B) {
	for _, n := range []rawtopiccommon.Offset{10000, 30000} {
		b.Run(fmt.Sprintf("ranges=%d", n), func(b *testing.B) {
			for range b.N {
				tracker := newCommitMessageTracker(0)
				for i := range n {
					tracker.Queue(i, i+1)
				}
				for i := range n {
					tracker.Acknowledge(i + 1)
				}
			}
		})
	}
}

func BenchmarkCommitAdversarialPartialAcknowledgement(b *testing.B) {
	const n rawtopiccommon.Offset = 100_000
	b.ReportAllocs()
	for range b.N {
		b.StopTimer()
		tracker := newCommitMessageTracker(0)
		tracker.Queue(n+1, n+2)
		for end := n; end > 0; end-- {
			tracker.Queue(end-1, end)
		}
		b.StartTimer()
		got := tracker.Acknowledge(n)
		b.StopTimer()
		require.EqualValues(b, n, got)
		require.EqualValues(b, 1, tracker.Acknowledge(n+2))
	}
}

func BenchmarkCommitTrackerQueue(b *testing.B) {
	b.Run("in_order", func(b *testing.B) {
		tracker := newCommitMessageTracker(0)
		tracker.Queue(0, 1)
		tracker.Queue(1, 2)
		b.ReportAllocs()
		for b.Loop() {
			// Measure Queue with spare ordered capacity, excluding slice growth.
			tracker.Queue(1, 2)
			tracker.ordered = tracker.ordered[:1]
			tracker.total = 1
		}
	})
	b.Run("out_of_order", func(b *testing.B) {
		tracker := newCommitMessageTracker(0)
		tracker.Queue(2, 3)
		b.ReportAllocs()
		for b.Loop() {
			tracker.Queue(0, 1)
			tracker.pending = nil
			tracker.total = 1
		}
	})
}

func newCommitMessageTracker(offset rawtopiccommon.Offset) *commitMessageTracker {
	session := &PartitionSession{}
	session.SetInitialCommitOffset(offset)

	return &commitMessageTracker{session: session}
}
