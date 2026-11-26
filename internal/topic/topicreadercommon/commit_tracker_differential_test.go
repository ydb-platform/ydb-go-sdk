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
	queue := func(r span, step int) {
		var count int64
		if r.end > watermark && r.end > r.start {
			count = int64(r.end - r.start)
			pending = append(pending, r)
			highest = max(highest, r.end)
		}
		require.Equal(t, count, tracker.Queue(r.start, r.end), "step %d, span %v", step, r)
	}
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
		queue(last, step)
	}
	acknowledge(highest, steps)

	// Keep the ordered sentinel and a higher out-of-order node pending through
	// the first split; the second split covers only the out-of-order index.
	sentinel := highest + 4096
	queue(span{sentinel - 1, sentinel}, steps+1)
	for end := sentinel - 1; end > watermark; end-- {
		queue(span{end - 1, end}, steps+1)
		if end%8 == 0 {
			queue(span{end - 3, end}, steps+1)
		}
	}
	acknowledge(sentinel-2, steps+2)
	acknowledge(sentinel-1, steps+3)
	acknowledge(sentinel, steps+4)
}
