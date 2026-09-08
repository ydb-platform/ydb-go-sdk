package topicreadercommon

import (
	"sort"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// PublicCommitRangeGetter return data piece for commit messages range
type PublicCommitRangeGetter interface {
	getCommitRange() PublicCommitRange
}

type CommitRanges struct {
	Ranges []CommitRange
}

func (r *CommitRanges) Len() int {
	return len(r.Ranges)
}

// GetCommitsInfo implements trace.TopicReaderStreamSendCommitMessageStartMessageInfo
func (r *CommitRanges) GetCommitsInfo() []trace.TopicReaderStreamCommitInfo {
	res := make([]trace.TopicReaderStreamCommitInfo, len(r.Ranges))
	for i := range res {
		res[i] = trace.TopicReaderStreamCommitInfo{
			Topic:              r.Ranges[i].PartitionSession.Topic,
			PartitionID:        r.Ranges[i].PartitionSession.PartitionID,
			PartitionSessionID: r.Ranges[i].PartitionSession.StreamPartitionSessionID.ToInt64(),
			StartOffset:        r.Ranges[i].CommitOffsetStart.ToInt64(),
			EndOffset:          r.Ranges[i].CommitOffsetEnd.ToInt64(),
		}
	}

	return res
}

func (r *CommitRanges) ToRawMessage() *rawtopicreader.CommitOffsetRequest {
	res := &rawtopicreader.CommitOffsetRequest{}

	res.CommitOffsets = r.ToPartitionsOffsets()

	return res
}

func NewCommitRangesWithCapacity(capacity int) CommitRanges {
	return CommitRanges{Ranges: make([]CommitRange, 0, capacity)}
}

func NewCommitRangesFromPublicCommits(ranges []PublicCommitRange) CommitRanges {
	res := CommitRanges{}
	res.Ranges = make([]CommitRange, len(ranges))
	for i := 0; i < len(res.Ranges); i++ {
		res.Ranges[i] = commitRangeSnapshot(ranges[i].priv)
	}

	return res
}

func (r *CommitRanges) Append(ranges ...PublicCommitRangeGetter) {
	converted := make([]CommitRange, len(ranges))

	for i := range ranges {
		converted[i] = commitRangeSnapshot(ranges[i].getCommitRange().priv)
		r.Ranges = append(r.Ranges, converted...)
	}
}

func (r *CommitRanges) AppendMessages(messages ...PublicMessage) {
	converted := make([]CommitRange, len(messages))

	for i := range messages {
		converted[i] = messages[i].getCommitRange().priv
		r.Ranges = append(r.Ranges, converted...)
	}
}

func (r *CommitRanges) AppendCommitRange(cr CommitRange) {
	r.Ranges = append(r.Ranges, commitRangeSnapshot(cr))
}

func (r *CommitRanges) AppendCommitRanges(ranges []CommitRange) {
	converted := make([]CommitRange, len(ranges))
	for i := range ranges {
		converted[i] = commitRangeSnapshot(ranges[i])
	}
	r.Ranges = append(r.Ranges, converted...)
}

func (r *CommitRanges) Reset() {
	r.Ranges = r.Ranges[:0]
}

func (r *CommitRanges) ToPartitionsOffsets() []rawtopicreader.PartitionCommitOffset {
	if len(r.Ranges) == 0 {
		return nil
	}

	r.Optimize()

	return r.toRawPartitionCommitOffset()
}

func (r *CommitRanges) Optimize() {
	if r.Len() == 0 {
		return
	}
	for i := range r.Ranges {
		r.Ranges[i] = commitRangeSnapshot(r.Ranges[i])
	}

	sort.Slice(r.Ranges, func(i, j int) bool {
		cI, cJ := &r.Ranges[i], &r.Ranges[j]
		switch {
		case cI.PartitionSession.StreamPartitionSessionID < cJ.PartitionSession.StreamPartitionSessionID:
			return true
		case cJ.PartitionSession.StreamPartitionSessionID < cI.PartitionSession.StreamPartitionSessionID:
			return false
		case cI.CommitOffsetStart < cJ.CommitOffsetStart:
			return true
		default:
			return false
		}
	})

	newCommits := r.Ranges[:1]
	lastCommit := &newCommits[0]
	for i := 1; i < len(r.Ranges); i++ {
		commit := &r.Ranges[i]
		if lastCommit.PartitionSession.StreamPartitionSessionID == commit.PartitionSession.StreamPartitionSessionID &&
			lastCommit.CommitOffsetEnd == commit.CommitOffsetStart {
			lastCommit.CommitOffsetEnd = commit.CommitOffsetEnd
			lastCommit.messageMetadata = mergeCommitMessageMetadata(
				lastCommit.messageMetadata,
				commit.messageMetadata,
			)
		} else {
			newCommits = append(newCommits, *commit)
			lastCommit = &newCommits[len(newCommits)-1]
		}
	}

	r.Ranges = newCommits
}

func (r *CommitRanges) toRawPartitionCommitOffset() []rawtopicreader.PartitionCommitOffset {
	if len(r.Ranges) == 0 {
		return nil
	}

	newPartition := func(id rawtopicreader.PartitionSessionID) rawtopicreader.PartitionCommitOffset {
		return rawtopicreader.PartitionCommitOffset{
			PartitionSessionID: id,
		}
	}

	partitionOffsets := make([]rawtopicreader.PartitionCommitOffset, 0, len(r.Ranges))
	partitionOffsets = append(partitionOffsets, newPartition(r.Ranges[0].PartitionSession.StreamPartitionSessionID))
	partition := &partitionOffsets[0]

	for i := range r.Ranges {
		commit := &r.Ranges[i]
		offsetsRange := rawtopiccommon.OffsetRange{
			Start: commit.CommitOffsetStart,
			End:   commit.CommitOffsetEnd,
		}
		if partition.PartitionSessionID != commit.PartitionSession.StreamPartitionSessionID {
			partitionOffsets = append(partitionOffsets, newPartition(commit.PartitionSession.StreamPartitionSessionID))
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
	priv CommitRange
}

func (p PublicCommitRange) getCommitRange() PublicCommitRange {
	p.priv = commitRangeSnapshot(p.priv)

	return p
}

type CommitRange struct {
	CommitOffsetStart rawtopiccommon.Offset
	CommitOffsetEnd   rawtopiccommon.Offset
	PartitionSession  *PartitionSession

	messageMetadata []commitMessageMetadata
}

func (c CommitRange) getCommitRange() PublicCommitRange {
	c = commitRangeSnapshot(c)

	return PublicCommitRange{priv: c}
}

func (c *CommitRange) session() *PartitionSession {
	return c.PartitionSession
}

func GetCommitRange(item PublicCommitRangeGetter) CommitRange {
	return item.getCommitRange().priv
}

// commitRangeSnapshot prevents append operations on a copied range from
// modifying spare capacity owned by the source batch or range.
func commitRangeSnapshot(cr CommitRange) CommitRange {
	if cr.messageMetadata != nil {
		cr.messageMetadata = cr.messageMetadata[:len(cr.messageMetadata):len(cr.messageMetadata)]
	}

	return cr
}
