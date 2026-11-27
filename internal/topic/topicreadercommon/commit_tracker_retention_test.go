package topicreadercommon

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
)

func TestCommitMessageTrackerRetainsBoundedHeapForDuplicateEnds(t *testing.T) {
	const submissions = 500_000
	tracker := newCommitMessageTracker(0)
	before := commitTrackerLiveHeap()
	for range submissions {
		tracker.Queue(0, 1)
	}

	retained := commitTrackerLiveHeap() - before
	require.EqualValues(t, submissions, tracker.Acknowledge(1))
	t.Logf("extra live heap with %d duplicate submissions: %d bytes", submissions, retained)
	require.Less(t, retained, int64(256<<10))
}

func TestCommitMessageTrackerReleasesCoveredOrderedRangesAfterPartialAck(t *testing.T) {
	const (
		burst rawtopiccommon.Offset = 500_000
		top   rawtopiccommon.Offset = burst + 2
		slack                       = 256 << 10
	)
	for _, tc := range []struct {
		name string
		ack  rawtopiccommon.Offset
	}{
		{name: "one_survivor", ack: burst},
		{name: "half_survive", ack: burst / 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tracker := newCommitMessageTracker(0)
			before := commitTrackerLiveHeap()

			for end := rawtopiccommon.Offset(1); end <= burst; end++ {
				tracker.Queue(end-1, end)
			}
			tracker.Queue(top-1, top)
			require.EqualValues(t, tc.ack, tracker.Acknowledge(tc.ack))

			retained := commitTrackerLiveHeap() - before
			survivors := int64(burst - tc.ack + 1)
			require.Equal(t, survivors, tracker.Acknowledge(top))
			t.Logf("extra live heap with %d pending ranges: %d bytes", survivors, retained)
			limit := int64(slack)
			if survivors > 1 {
				limit += 16 * survivors * 3 / 2
			}
			require.Less(t, retained, limit)
		})
	}
}

func TestCommitMessageTrackerReleasesCoveredBurstDuringPartialProgress(t *testing.T) {
	const (
		burst  rawtopiccommon.Offset = 500_000
		steady rawtopiccommon.Offset = 100_000
		top    rawtopiccommon.Offset = burst + steady + 1
	)
	tracker := newCommitMessageTracker(0)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	tracker.Queue(top-1, top)
	for end := burst; end > 0; end-- {
		tracker.Queue(end-1, end)
	}
	require.EqualValues(t, burst-1, tracker.Acknowledge(burst-1))
	for end := burst + 1; end <= burst+steady; end++ {
		tracker.Queue(end-1, end)
		if count := tracker.Acknowledge(end - 1); count != 1 {
			t.Fatalf("ACK %d completed %d ranges, want 1", end-1, count)
		}
	}

	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&after)
	require.EqualValues(t, 2, tracker.Acknowledge(top))
	retained := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	t.Logf("extra live heap with two pending spans: %d bytes", retained)
	require.Less(t, retained, int64(1<<20))
}

func commitTrackerLiveHeap() int64 {
	runtime.GC()
	runtime.GC()

	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)

	return int64(stats.HeapAlloc)
}
