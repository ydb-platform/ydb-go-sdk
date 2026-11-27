package topicreadercommon

import (
	"slices"
	"sort"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
)

const (
	orderedCommitChunkSize  = 512
	orderedCommitCompactMin = 64
)

// orderedCommitRanges stores increasing ends with counts cumulative across chunks.
// Only the first live chunk can have consumed entries; earlier chunk headers are nil.
type orderedCommitRanges struct {
	chunks                 [][]commitMetricRange
	firstChunk, firstEntry int
	prefixBase             int64 // cumulative count before the first live entry
}

// queue returns false for ends that belong in the out-of-order index.
func (q *orderedCommitRanges) queue(r commitMetricRange) bool {
	if len(q.chunks) == 0 {
		// Grow the first chunk on demand so short-lived trackers pay only for what they use.
		q.chunks = append(q.chunks, []commitMetricRange{r})

		return true
	}

	i := len(q.chunks) - 1
	chunk := q.chunks[i]
	last := lastCommitRange(chunk)
	switch {
	case r.end < last.end:
		return false
	case r.end == last.end:
		last.count += r.count
	default:
		r.count += last.count
		if len(chunk) == orderedCommitChunkSize {
			chunk = make([]commitMetricRange, 1, orderedCommitChunkSize)
			chunk[0] = r
			q.chunks = append(q.chunks, chunk)
		} else {
			q.chunks[i] = append(chunk, r)
		}
	}

	return true
}

// removeThrough handles partial ACKs only: the last ordered end stays pending.
func (q *orderedCommitRanges) removeThrough(end rawtopiccommon.Offset) int64 {
	chunks := q.chunks[q.firstChunk:]
	if end < chunks[0][q.firstEntry].end {
		return 0
	}
	i := sort.Search(len(chunks), func(i int) bool {
		return lastCommitRange(chunks[i]).end > end
	})
	chunk := chunks[i]
	j := sort.Search(len(chunk), func(j int) bool {
		return chunk[j].end > end
	})

	var prefix int64
	if j > 0 {
		prefix = chunk[j-1].count
	} else {
		prefix = lastCommitRange(chunks[i-1]).count
	}
	count := prefix - q.prefixBase
	q.prefixBase, q.firstEntry = prefix, j
	if i > 0 {
		clear(chunks[:i])
		q.firstChunk += i
		// Shrink only the chunk directory, never copy surviving entries.
		if q.firstChunk >= orderedCommitCompactMin && q.firstChunk >= len(chunks)-i {
			q.chunks = slices.Clone(chunks[i:])
			q.firstChunk = 0
		}
	}

	return count
}

func lastCommitRange(chunk []commitMetricRange) *commitMetricRange {
	return &chunk[len(chunk)-1]
}
