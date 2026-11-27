package topicreadercommon

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
)

func commitRangeDepth(n *commitRangeNode) int {
	if n == nil {
		return 0
	}

	return 1 + max(commitRangeDepth(n.left), commitRangeDepth(n.right))
}

// A fixed priority sequence lets a caller pick an out-of-order admission order
// that turns the index into a list. Replay that order for the xorshift stream
// seeded with 1 and check that the index stays shallow.
func TestCommitRangeIndexStaysShallowForPredictableAdmissionOrder(t *testing.T) {
	const ranges = 5000
	priorities := make([]uint64, ranges)
	state := uint64(1)
	for i := range priorities {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		priorities[i] = state
	}
	sorted := slices.Clone(priorities)
	slices.Sort(sorted)

	const sentinel rawtopiccommon.Offset = ranges + 10
	tracker := newCommitMessageTracker(0)
	require.EqualValues(t, 1, tracker.Queue(sentinel-1, sentinel))
	for _, priority := range priorities {
		rank, _ := slices.BinarySearch(sorted, priority)
		end := rawtopiccommon.Offset(ranges - rank)
		require.EqualValues(t, 1, tracker.Queue(end-1, end))
	}

	require.Less(t, commitRangeDepth(tracker.pending), 100)
	require.EqualValues(t, ranges, tracker.Acknowledge(ranges))
	require.EqualValues(t, 1, tracker.Acknowledge(sentinel))
}
