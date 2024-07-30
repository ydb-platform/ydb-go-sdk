package topicreadercommon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
)

var _ PublicCommitRangeGetter = &PublicMessage{}

var _ PublicCommitRangeGetter = &PublicBatch{}

func TestCompressCommitsInplace(t *testing.T) {
	session1 := &PartitionSession{StreamPartitionSessionID: 1}
	session2 := &PartitionSession{StreamPartitionSessionID: 2}
	table := []struct {
		name     string
		source   []CommitRange
		expected []CommitRange
	}{
		{
			name:     "Empty",
			source:   nil,
			expected: nil,
		},
		{
			name: "OneCommit",
			source: []CommitRange{
				{
					CommitOffsetStart: 1,
					CommitOffsetEnd:   2,
					PartitionSession:  session1,
				},
			},
			expected: []CommitRange{
				{
					CommitOffsetStart: 1,
					CommitOffsetEnd:   2,
					PartitionSession:  session1,
				},
			},
		},
		{
			name: "CompressedToOne",
			source: []CommitRange{
				{
					CommitOffsetStart: 1,
					CommitOffsetEnd:   2,
					PartitionSession:  session1,
				},
				{
					CommitOffsetStart: 2,
					CommitOffsetEnd:   5,
					PartitionSession:  session1,
				},
				{
					CommitOffsetStart: 5,
					CommitOffsetEnd:   10,
					PartitionSession:  session1,
				},
			},
			expected: []CommitRange{
				{
					CommitOffsetStart: 1,
					CommitOffsetEnd:   10,
					PartitionSession:  session1,
				},
			},
		},
		{
			name: "CompressedUnordered",
			source: []CommitRange{
				{
					CommitOffsetStart: 5,
					CommitOffsetEnd:   10,
					PartitionSession:  session1,
				},
				{
					CommitOffsetStart: 2,
					CommitOffsetEnd:   5,
					PartitionSession:  session1,
				},
				{
					CommitOffsetStart: 1,
					CommitOffsetEnd:   2,
					PartitionSession:  session1,
				},
			},
			expected: []CommitRange{
				{
					CommitOffsetStart: 1,
					CommitOffsetEnd:   10,
					PartitionSession:  session1,
				},
			},
		},
		{
			name: "CompressDifferentSessionsSeparated",
			source: []CommitRange{
				{
					CommitOffsetStart: 1,
					CommitOffsetEnd:   2,
					PartitionSession:  session1,
				},
				{
					CommitOffsetStart: 2,
					CommitOffsetEnd:   3,
					PartitionSession:  session2,
				},
			},
			expected: []CommitRange{
				{
					CommitOffsetStart: 1,
					CommitOffsetEnd:   2,
					PartitionSession:  session1,
				},
				{
					CommitOffsetStart: 2,
					CommitOffsetEnd:   3,
					PartitionSession:  session2,
				},
			},
		},
		{
			name: "CompressTwoSessions",
			source: []CommitRange{
				{
					CommitOffsetStart: 1,
					CommitOffsetEnd:   1,
					PartitionSession:  session1,
				},
				{
					CommitOffsetStart: 2,
					CommitOffsetEnd:   3,
					PartitionSession:  session2,
				},
				{
					CommitOffsetStart: 1,
					CommitOffsetEnd:   3,
					PartitionSession:  session1,
				},
				{
					CommitOffsetStart: 3,
					CommitOffsetEnd:   5,
					PartitionSession:  session2,
				},
			},
			expected: []CommitRange{
				{
					CommitOffsetStart: 1,
					CommitOffsetEnd:   3,
					PartitionSession:  session1,
				},
				{
					CommitOffsetStart: 2,
					CommitOffsetEnd:   5,
					PartitionSession:  session2,
				},
			},
		},
	}
	for _, test := range table {
		t.Run(test.name, func(t *testing.T) {
			var v CommitRanges
			v.Ranges = test.source
			v.Optimize()
			require.Equal(t, test.expected, v.Ranges)
		})
	}
}

func TestCommitsToRawPartitionCommitOffset(t *testing.T) {
	session1 := &PartitionSession{StreamPartitionSessionID: 1}
	session2 := &PartitionSession{StreamPartitionSessionID: 2}

	table := []struct {
		name     string
		source   []CommitRange
		expected []rawtopicreader.PartitionCommitOffset
	}{
		{
			name:     "Empty",
			source:   nil,
			expected: nil,
		},
		{
			name: "OneCommit",
			source: []CommitRange{
				{CommitOffsetStart: 1, CommitOffsetEnd: 2, PartitionSession: session1},
			},
			expected: []rawtopicreader.PartitionCommitOffset{
				{
					PartitionSessionID: 1,
					Offsets: []rawtopiccommon.OffsetRange{
						{Start: 1, End: 2},
					},
				},
			},
		},
		{
			name: "NeighboursWithOneSession",
			source: []CommitRange{
				{CommitOffsetStart: 1, CommitOffsetEnd: 2, PartitionSession: session1},
				{CommitOffsetStart: 10, CommitOffsetEnd: 20, PartitionSession: session1},
				{CommitOffsetStart: 30, CommitOffsetEnd: 40, PartitionSession: session1},
			},
			expected: []rawtopicreader.PartitionCommitOffset{
				{
					PartitionSessionID: 1,
					Offsets: []rawtopiccommon.OffsetRange{
						{Start: 1, End: 2},
						{Start: 10, End: 20},
						{Start: 30, End: 40},
					},
				},
			},
		},
		{
			name: "TwoSessionsSameOffsets",
			source: []CommitRange{
				{CommitOffsetStart: 1, CommitOffsetEnd: 2, PartitionSession: session1},
				{CommitOffsetStart: 10, CommitOffsetEnd: 20, PartitionSession: session1},
				{CommitOffsetStart: 1, CommitOffsetEnd: 2, PartitionSession: session2},
				{CommitOffsetStart: 10, CommitOffsetEnd: 20, PartitionSession: session2},
			},
			expected: []rawtopicreader.PartitionCommitOffset{
				{
					PartitionSessionID: 1,
					Offsets: []rawtopiccommon.OffsetRange{
						{Start: 1, End: 2},
						{Start: 10, End: 20},
					},
				},
				{
					PartitionSessionID: 2,
					Offsets: []rawtopiccommon.OffsetRange{
						{Start: 1, End: 2},
						{Start: 10, End: 20},
					},
				},
			},
		},
		{
			name: "TwoSessionsWithDifferenceOffsets",
			source: []CommitRange{
				{CommitOffsetStart: 1, CommitOffsetEnd: 2, PartitionSession: session1},
				{CommitOffsetStart: 10, CommitOffsetEnd: 20, PartitionSession: session1},
				{CommitOffsetStart: 1, CommitOffsetEnd: 2, PartitionSession: session2},
				{CommitOffsetStart: 3, CommitOffsetEnd: 4, PartitionSession: session2},
				{CommitOffsetStart: 5, CommitOffsetEnd: 6, PartitionSession: session2},
			},
			expected: []rawtopicreader.PartitionCommitOffset{
				{
					PartitionSessionID: 1,
					Offsets: []rawtopiccommon.OffsetRange{
						{Start: 1, End: 2},
						{Start: 10, End: 20},
					},
				},
				{
					PartitionSessionID: 2,
					Offsets: []rawtopiccommon.OffsetRange{
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
			v.Ranges = test.source
			res := v.toRawPartitionCommitOffset()
			require.Equal(t, test.expected, res)
		})
	}
}
