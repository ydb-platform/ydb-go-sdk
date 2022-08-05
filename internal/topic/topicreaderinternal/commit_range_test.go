package topicreaderinternal

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
)

var _ PublicCommitRangeGetter = &PublicMessage{}

var _ PublicCommitRangeGetter = &PublicBatch{}

func TestCompressCommitsInplace(t *testing.T) {
	session1 := &partitionSession{partitionSessionID: 1}
	session2 := &partitionSession{partitionSessionID: 2}
	table := []struct {
		name     string
		source   []commitRange
		expected []commitRange
	}{
		{
			name:     "Empty",
			source:   nil,
			expected: nil,
		},
		{
			name: "OneCommit",
			source: []commitRange{
				{
					commitOffsetStart: 1,
					commitOffsetEnd:   2,
					partitionSession:  session1,
				},
			},
			expected: []commitRange{
				{
					commitOffsetStart: 1,
					commitOffsetEnd:   2,
					partitionSession:  session1,
				},
			},
		},
		{
			name: "CompressedToOne",
			source: []commitRange{
				{
					commitOffsetStart: 1,
					commitOffsetEnd:   2,
					partitionSession:  session1,
				},
				{
					commitOffsetStart: 2,
					commitOffsetEnd:   5,
					partitionSession:  session1,
				},
				{
					commitOffsetStart: 5,
					commitOffsetEnd:   10,
					partitionSession:  session1,
				},
			},
			expected: []commitRange{
				{
					commitOffsetStart: 1,
					commitOffsetEnd:   10,
					partitionSession:  session1,
				},
			},
		},
		{
			name: "CompressedUnordered",
			source: []commitRange{
				{
					commitOffsetStart: 5,
					commitOffsetEnd:   10,
					partitionSession:  session1,
				},
				{
					commitOffsetStart: 2,
					commitOffsetEnd:   5,
					partitionSession:  session1,
				},
				{
					commitOffsetStart: 1,
					commitOffsetEnd:   2,
					partitionSession:  session1,
				},
			},
			expected: []commitRange{
				{
					commitOffsetStart: 1,
					commitOffsetEnd:   10,
					partitionSession:  session1,
				},
			},
		},
		{
			name: "CompressDifferentSessionsSeparated",
			source: []commitRange{
				{
					commitOffsetStart: 1,
					commitOffsetEnd:   2,
					partitionSession:  session1,
				},
				{
					commitOffsetStart: 2,
					commitOffsetEnd:   3,
					partitionSession:  session2,
				},
			},
			expected: []commitRange{
				{
					commitOffsetStart: 1,
					commitOffsetEnd:   2,
					partitionSession:  session1,
				},
				{
					commitOffsetStart: 2,
					commitOffsetEnd:   3,
					partitionSession:  session2,
				},
			},
		},
		{
			name: "CompressTwoSessions",
			source: []commitRange{
				{
					commitOffsetStart: 1,
					commitOffsetEnd:   1,
					partitionSession:  session1,
				},
				{
					commitOffsetStart: 2,
					commitOffsetEnd:   3,
					partitionSession:  session2,
				},
				{
					commitOffsetStart: 1,
					commitOffsetEnd:   3,
					partitionSession:  session1,
				},
				{
					commitOffsetStart: 3,
					commitOffsetEnd:   5,
					partitionSession:  session2,
				},
			},
			expected: []commitRange{
				{
					commitOffsetStart: 1,
					commitOffsetEnd:   3,
					partitionSession:  session1,
				},
				{
					commitOffsetStart: 2,
					commitOffsetEnd:   5,
					partitionSession:  session2,
				},
			},
		},
	}
	for _, test := range table {
		t.Run(test.name, func(t *testing.T) {
			var v CommitRanges
			v.ranges = test.source
			v.optimize()
			require.Equal(t, test.expected, v.ranges)
		})
	}
}

func TestCommitsToRawPartitionCommitOffset(t *testing.T) {
	session1 := &partitionSession{partitionSessionID: 1}
	session2 := &partitionSession{partitionSessionID: 2}

	table := []struct {
		name     string
		source   []commitRange
		expected []rawtopicreader.PartitionCommitOffset
	}{
		{
			name:     "Empty",
			source:   nil,
			expected: nil,
		},
		{
			name: "OneCommit",
			source: []commitRange{
				{commitOffsetStart: 1, commitOffsetEnd: 2, partitionSession: session1},
			},
			expected: []rawtopicreader.PartitionCommitOffset{
				{
					PartitionSessionID: 1,
					Offsets: []rawtopicreader.OffsetRange{
						{Start: 1, End: 2},
					},
				},
			},
		},
		{
			name: "NeighboursWithOneSession",
			source: []commitRange{
				{commitOffsetStart: 1, commitOffsetEnd: 2, partitionSession: session1},
				{commitOffsetStart: 10, commitOffsetEnd: 20, partitionSession: session1},
				{commitOffsetStart: 30, commitOffsetEnd: 40, partitionSession: session1},
			},
			expected: []rawtopicreader.PartitionCommitOffset{
				{
					PartitionSessionID: 1,
					Offsets: []rawtopicreader.OffsetRange{
						{Start: 1, End: 2},
						{Start: 10, End: 20},
						{Start: 30, End: 40},
					},
				},
			},
		},
		{
			name: "TwoSessionsSameOffsets",
			source: []commitRange{
				{commitOffsetStart: 1, commitOffsetEnd: 2, partitionSession: session1},
				{commitOffsetStart: 10, commitOffsetEnd: 20, partitionSession: session1},
				{commitOffsetStart: 1, commitOffsetEnd: 2, partitionSession: session2},
				{commitOffsetStart: 10, commitOffsetEnd: 20, partitionSession: session2},
			},
			expected: []rawtopicreader.PartitionCommitOffset{
				{
					PartitionSessionID: 1,
					Offsets: []rawtopicreader.OffsetRange{
						{Start: 1, End: 2},
						{Start: 10, End: 20},
					},
				},
				{
					PartitionSessionID: 2,
					Offsets: []rawtopicreader.OffsetRange{
						{Start: 1, End: 2},
						{Start: 10, End: 20},
					},
				},
			},
		},
		{
			name: "TwoSessionsWithDifferenceOffsets",
			source: []commitRange{
				{commitOffsetStart: 1, commitOffsetEnd: 2, partitionSession: session1},
				{commitOffsetStart: 10, commitOffsetEnd: 20, partitionSession: session1},
				{commitOffsetStart: 1, commitOffsetEnd: 2, partitionSession: session2},
				{commitOffsetStart: 3, commitOffsetEnd: 4, partitionSession: session2},
				{commitOffsetStart: 5, commitOffsetEnd: 6, partitionSession: session2},
			},
			expected: []rawtopicreader.PartitionCommitOffset{
				{
					PartitionSessionID: 1,
					Offsets: []rawtopicreader.OffsetRange{
						{Start: 1, End: 2},
						{Start: 10, End: 20},
					},
				},
				{
					PartitionSessionID: 2,
					Offsets: []rawtopicreader.OffsetRange{
						{Start: 1, End: 2},
						{Start: 3, End: 4},
						{Start: 5, End: 6},
					},
				},
			},
		},
	}

	for _, test := range table {
		t.Run(test.name, func(t *testing.T) {
			var v CommitRanges
			v.ranges = test.source
			res := v.toRawPartitionCommitOffset()
			require.Equal(t, test.expected, res)
		})
	}
}

func testNewCommitRanges(commitable ...PublicCommitRangeGetter) *CommitRanges {
	var res CommitRanges
	res.Append(commitable...)
	return &res
}
