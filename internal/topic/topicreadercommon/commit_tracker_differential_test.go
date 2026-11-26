package topicreadercommon

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
)

func TestCommitMessageTrackerMatchesRangeScan(t *testing.T) {
	type span struct {
		start, end rawtopiccommon.Offset
	}

	random := rand.New(rand.NewPCG(2283, 7))
	tracker := newCommitMessageTracker(10)
	watermark, highest := rawtopiccommon.Offset(10), rawtopiccommon.Offset(10)
	var pending []span
	var last span
	acknowledge := func(offset rawtopiccommon.Offset, step int) {
		var count int64
		watermark = max(watermark, offset)
		remaining := pending[:0]
		for _, r := range pending {
			if r.end <= watermark {
				count += int64(r.end - r.start)
			} else {
				remaining = append(remaining, r)
			}
		}
		pending = remaining
		require.Equal(t, count, tracker.Acknowledge(offset), "step %d, ACK %d", step, offset)
		require.Equal(t, watermark, tracker.session.CommittedOffset(), "step %d", step)
	}
	const steps = 3000
	for step := range steps {
		switch random.IntN(8) {
		case 0:
			acknowledge(watermark+rawtopiccommon.Offset(random.IntN(int(highest-watermark)+1)), step)
			continue
		case 1:
			acknowledge(watermark-rawtopiccommon.Offset(random.IntN(4)), step)
			continue
		case 2:
			// Submit the previous span again, whether it is still pending or already covered.
		case 3, 4:
			end := watermark + rawtopiccommon.Offset(random.IntN(int(highest-watermark)+5)-2)
			last = span{start: end - rawtopiccommon.Offset(random.IntN(12)), end: end}
		default:
			end := highest + rawtopiccommon.Offset(random.IntN(4))
			last = span{start: end - rawtopiccommon.Offset(random.IntN(12)), end: end}
		}
		var count int64
		if last.end > watermark && last.end > last.start {
			count = int64(last.end - last.start)
			pending = append(pending, last)
			highest = max(highest, last.end)
		}
		require.Equal(t, count, tracker.Queue(last.start, last.end), "step %d, span %v", step, last)
	}
	acknowledge(highest, steps)
}
