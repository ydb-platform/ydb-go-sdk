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
