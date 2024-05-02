package topicreaderinternal

import (
	"sort"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// PublicCommitRangeGetter return data piece for commit messages range
type PublicCommitRangeGetter interface {
	getCommitRange() PublicCommitRange
}

type CommitRanges struct {
	ranges []commitRange
}

func (r *CommitRanges) len() int {
	return len(r.ranges)
}

// GetCommitsInfo implements trace.TopicReaderStreamSendCommitMessageStartMessageInfo
func (r *CommitRanges) GetCommitsInfo() []trace.TopicReaderStreamCommitInfo {
	res := make([]trace.TopicReaderStreamCommitInfo, len(r.ranges))
	for i := range res {
		res[i] = trace.TopicReaderStreamCommitInfo{
			Topic:              r.ranges[i].partitionSession.Topic,
			PartitionID:        r.ranges[i].partitionSession.PartitionID,
			PartitionSessionID: r.ranges[i].partitionSession.partitionSessionID.ToInt64(),
			StartOffset:        r.ranges[i].commitOffsetStart.ToInt64(),
			EndOffset:          r.ranges[i].commitOffsetEnd.ToInt64(),
		}
	}

	return res
}

func NewCommitRangesWithCapacity(capacity int) CommitRanges {
	return CommitRanges{ranges: make([]commitRange, 0, capacity)}
}

func NewCommitRangesFromPublicCommits(ranges []PublicCommitRange) CommitRanges {
	res := CommitRanges{}
	res.ranges = make([]commitRange, len(ranges))
	for i := 0; i < len(res.ranges); i++ {
		res.ranges[i] = ranges[i].priv
	}

	return res
}

func (r *CommitRanges) Append(ranges ...PublicCommitRangeGetter) {
	converted := make([]commitRange, len(ranges))

	for i := range ranges {
		converted[i] = ranges[i].getCommitRange().priv
		r.ranges = append(r.ranges, converted...)
	}
}

func (r *CommitRanges) AppendMessages(messages ...PublicMessage) {
	converted := make([]commitRange, len(messages))

	for i := range messages {
		converted[i] = messages[i].getCommitRange().priv
		r.ranges = append(r.ranges, converted...)
	}
}

func (r *CommitRanges) appendCommitRange(cr commitRange) {
	r.ranges = append(r.ranges, cr)
}

func (r *CommitRanges) appendCommitRanges(ranges []commitRange) {
	r.ranges = append(r.ranges, ranges...)
}

func (r *CommitRanges) Reset() {
	r.ranges = r.ranges[:0]
}

func (r *CommitRanges) toPartitionsOffsets() []rawtopicreader.PartitionCommitOffset {
	if len(r.ranges) == 0 {
		return nil
	}

	r.optimize()

	return r.toRawPartitionCommitOffset()
}

func (r *CommitRanges) optimize() {
	if r.len() == 0 {
		return
	}

	sort.Slice(r.ranges, func(i, j int) bool {
		cI, cJ := &r.ranges[i], &r.ranges[j]
		switch {
		case cI.partitionSession.partitionSessionID < cJ.partitionSession.partitionSessionID:
			return true
		case cJ.partitionSession.partitionSessionID < cI.partitionSession.partitionSessionID:
			return false
		case cI.commitOffsetStart < cJ.commitOffsetStart:
			return true
		default:
			return false
		}
	})

	newCommits := r.ranges[:1]
	lastCommit := &newCommits[0]
	for i := 1; i < len(r.ranges); i++ {
		commit := &r.ranges[i]
		if lastCommit.partitionSession.partitionSessionID == commit.partitionSession.partitionSessionID &&
			lastCommit.commitOffsetEnd == commit.commitOffsetStart {
			lastCommit.commitOffsetEnd = commit.commitOffsetEnd
		} else {
			newCommits = append(newCommits, *commit)
			lastCommit = &newCommits[len(newCommits)-1]
		}
	}

	r.ranges = newCommits
}

func (r *CommitRanges) toRawPartitionCommitOffset() []rawtopicreader.PartitionCommitOffset {
	if len(r.ranges) == 0 {
		return nil
	}

	newPartition := func(id rawtopicreader.PartitionSessionID) rawtopicreader.PartitionCommitOffset {
		return rawtopicreader.PartitionCommitOffset{
			PartitionSessionID: id,
		}
	}

	partitionOffsets := make([]rawtopicreader.PartitionCommitOffset, 0, len(r.ranges))
	partitionOffsets = append(partitionOffsets, newPartition(r.ranges[0].partitionSession.partitionSessionID))
	partition := &partitionOffsets[0]

	for i := range r.ranges {
		commit := &r.ranges[i]
		offsetsRange := rawtopicreader.OffsetRange{
			Start: commit.commitOffsetStart,
			End:   commit.commitOffsetEnd,
		}
		if partition.PartitionSessionID != commit.partitionSession.partitionSessionID {
			partitionOffsets = append(partitionOffsets, newPartition(commit.partitionSession.partitionSessionID))
			partition = &partitionOffsets[len(partitionOffsets)-1]
		}
		partition.Offsets = append(partition.Offsets, offsetsRange)
	}

	return partitionOffsets
}

// PublicCommitRange contains data for commit messages range
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
type PublicCommitRange struct {
	priv commitRange
}

type commitRange struct {
	commitOffsetStart rawtopicreader.Offset
	commitOffsetEnd   rawtopicreader.Offset
	partitionSession  *partitionSession
}

func (c commitRange) getCommitRange() PublicCommitRange {
	return PublicCommitRange{priv: c}
}

func (c *commitRange) session() *partitionSession {
	return c.partitionSession
}
