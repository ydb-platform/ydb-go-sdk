package stubs

import (
	"encoding/binary"
	"sync"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

// DescribeWithSplitsState holds topic description and records partition splits.
// When a split is recorded, the next Describe returns the parent with ChildPartitionIDs set
// and two new partitions with bounds [from, mid) and [mid, to) where mid = (from+to)/2.
type DescribeWithSplitsState struct {
	t testing.TB

	mu     sync.Mutex
	base   topictypes.TopicDescription
	splits map[int64]partitionSplit // partitionID -> split info
	nextID int64                    // next free partition ID for new children
}

type partitionSplit struct {
	child1, child2 int64
	from1, to1     []byte // first child: [from1, to1)
	from2, to2     []byte // second child: [from2, to2)
}

// NewDescribeWithSplitsState creates state with the given base description.
// nextPartitionID is the first ID to use for new partitions (e.g. len(base.Partitions)+1).
func NewDescribeWithSplitsState(t testing.TB, base topictypes.TopicDescription, nextPartitionID int64) *DescribeWithSplitsState {
	t.Helper()

	return &DescribeWithSplitsState{
		t:      t,
		base:   base,
		splits: make(map[int64]partitionSplit),
		nextID: nextPartitionID,
	}
}

// RecordSplit records that the given partition has split into two children.
// Bounds are computed as [from, (from+to)/2) and [(from+to)/2, to).
// GetDescription will then return the parent with ChildPartitionIDs and the two new partitions.
func (s *DescribeWithSplitsState) RecordSplit(partitionID int64) {
	s.t.Helper()

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.splits[partitionID]; exists {
		return // already split
	}

	var fromBound, toBound []byte
	for _, p := range s.base.Partitions {
		if p.PartitionID == partitionID {
			fromBound = p.FromBound
			toBound = p.ToBound

			break
		}
	}

	mid := midBound(fromBound, toBound)
	c1, c2 := s.nextID, s.nextID+1
	s.nextID += 2

	s.splits[partitionID] = partitionSplit{
		child1: c1,
		child2: c2,
		from1:  cloneBytes(fromBound),
		to1:    cloneBytes(mid),
		from2:  cloneBytes(mid),
		to2:    cloneBytes(toBound),
	}
}

// GetDescription returns the current topic description: base partitions with
// ChildPartitionIDs set where splits occurred, plus all child partitions.
func (s *DescribeWithSplitsState) GetDescription() topictypes.TopicDescription {
	s.t.Helper()

	s.mu.Lock()
	defer s.mu.Unlock()

	out := topictypes.TopicDescription{
		Path:              s.base.Path,
		PartitionSettings: s.base.PartitionSettings,
		Partitions:        make([]topictypes.PartitionInfo, 0, len(s.base.Partitions)+2*len(s.splits)),
	}

	// Copy base partitions, updating ChildPartitionIDs for split ones
	for _, p := range s.base.Partitions {
		pi := p
		if split, ok := s.splits[p.PartitionID]; ok {
			pi.ChildPartitionIDs = []int64{split.child1, split.child2}
		}
		out.Partitions = append(out.Partitions, pi)
	}

	// Append child partitions
	for parentID, split := range s.splits {
		out.Partitions = append(out.Partitions,
			topictypes.PartitionInfo{
				PartitionID:        split.child1,
				ChildPartitionIDs:  []int64{},
				ParentPartitionIDs: []int64{parentID},
				FromBound:          split.from1,
				ToBound:            split.to1,
			},
			topictypes.PartitionInfo{
				PartitionID:        split.child2,
				ChildPartitionIDs:  []int64{},
				ParentPartitionIDs: []int64{parentID},
				FromBound:          split.from2,
				ToBound:            split.to2,
			},
		)
	}

	return out
}

// midBound returns (from+to)/2 as 8-byte big-endian. Empty from is treated as 0.
func midBound(from, to []byte) []byte {
	fromU := boundToU64(from)
	toU := boundToU64(to)
	mid := (fromU + toU) / 2

	return u64ToBound(mid)
}

func boundToU64(b []byte) uint64 {
	if len(b) == 0 {
		return 0
	}
	if len(b) < 8 {
		// pad left with zeros (big-endian)
		buf := make([]byte, 8)
		copy(buf[8-len(b):], b)

		return binary.BigEndian.Uint64(buf)
	}

	return binary.BigEndian.Uint64(b[:8])
}

func u64ToBound(u uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, u)

	return b
}

func cloneBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)

	return out
}
