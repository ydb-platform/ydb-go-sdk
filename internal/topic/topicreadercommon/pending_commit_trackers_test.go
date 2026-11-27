package topicreadercommon

import (
	"runtime"
	"testing"
	"weak"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
)

func TestPendingCommitTrackersReleaseHeapAfterFinalACKs(t *testing.T) {
	const burst = 500_000
	owner := &pendingCommitTrackers{}
	before := commitTrackerLiveHeap()
	trackers := make([]*commitMessageTracker, burst)
	for i := range trackers {
		tracker := newCommitMessageTracker(0)
		tracker.owner = owner
		if count := tracker.Queue(0, 1); count != 1 {
			t.Fatalf("tracker %d queued %d messages, want 1", i, count)
		}
		trackers[i] = tracker
	}
	ref := weak.Make(trackers[burst/2])
	for parity := range 2 {
		for i := parity; i < len(trackers); i += 2 {
			if count := trackers[i].Acknowledge(1); count != 1 {
				t.Fatalf("tracker %d ACKed %d messages, want 1", i, count)
			}
		}
	}
	retained := commitTrackerLiveHeap() - before
	runtime.KeepAlive(owner)
	t.Logf("retained after %d final ACKs: %d bytes", burst, retained)
	require.Nil(t, ref.Value())
	require.Less(t, retained, int64(256<<10))
}

func TestPendingCommitTrackersCloseDuringFinalACKs(t *testing.T) {
	const outstanding = 1024
	owner := &pendingCommitTrackers{}
	trackers := make([]*commitMessageTracker, outstanding)
	for i := range trackers {
		tracker := newCommitMessageTracker(0)
		tracker.owner = owner
		require.EqualValues(t, 1, tracker.Queue(0, 1))
		trackers[i] = tracker
	}

	start, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		<-start
		for i := 0; i < len(trackers); i += 2 {
			trackers[i].Acknowledge(1)
		}
	}()
	close(start)
	owner.close()
	xtest.WaitChannelClosed(t, done)

	for _, tracker := range trackers {
		require.Zero(t, tracker.Acknowledge(1))
		require.Zero(t, tracker.Queue(1, 2))
	}
}

func BenchmarkCommitMessageTrackerQueueFromEmpty(b *testing.B) {
	owner := &pendingCommitTrackers{}
	tracker := newCommitMessageTracker(0)
	tracker.owner = owner
	var offset rawtopiccommon.Offset
	b.ReportAllocs()
	for b.Loop() {
		end := offset + 1
		tracker.Queue(offset, end)
		tracker.Acknowledge(end)
		offset = end
		// Measure the first registration on a fresh owner each time.
		*owner = pendingCommitTrackers{}
	}
}
