package topicmultiwriter

import (
	"math/big"
	"testing"
)

func TestRangeFromShardNumber_SingleShard(t *testing.T) {
	left, right := rangeFromShardNumber(0, 1)

	if left.Cmp(big.NewInt(0)) != 0 {
		t.Fatalf("expected left=0 for single shard, got %v", left)
	}
	if right.Cmp(uint128Max) != 0 {
		t.Fatalf("expected right=uint128Max for single shard, got %v", right)
	}
}

func TestRangeFromShardNumber_MultipleShards(t *testing.T) {
	const shardCount = 4

	var prevRight *big.Int
	for shard := range shardCount {
		left, right := rangeFromShardNumber(shard, shardCount)

		if left.Sign() < 0 {
			t.Fatalf("left bound must be non-negative, got %v for shard %d", left, shard)
		}
		if right.Cmp(uint128Max) > 0 {
			t.Fatalf("right bound must be <= uint128Max, got %v for shard %d", right, shard)
		}
		if left.Cmp(right) > 0 {
			t.Fatalf("left must be <= right for shard %d: left=%v right=%v", shard, left, right)
		}

		if shard == shardCount-1 {
			if right.Cmp(uint128Max) != 0 {
				t.Fatalf("last shard must end at uint128Max, got %v", right)
			}
		}

		if shard > 0 {
			expectedLeft := new(big.Int).Add(prevRight, big.NewInt(1))
			if left.Cmp(expectedLeft) != 0 {
				t.Fatalf("expected shard %d left=%v (prevRight+1), got %v", shard, expectedLeft, left)
			}
		}

		prevRight = right
	}
}

func TestRangeFromShardNumber_InvalidShardPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic for invalid shardNumber, got none")
		}
	}()

	_ = func() *big.Int {
		left, _ := rangeFromShardNumber(-1, 4)

		return left
	}()
}

func TestAsKeyBound_ValidValues(t *testing.T) {
	tests := []struct {
		name string
		v    *big.Int
	}{
		{name: "zero", v: big.NewInt(0)},
		{name: "one", v: big.NewInt(1)},
		{name: "uint128Max", v: new(big.Int).Set(uint128Max)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := asKeyBound(tt.v)
			if len(b) != 16 {
				t.Fatalf("expected length 16, got %d", len(b))
			}

			got := new(big.Int).SetBytes(b)
			if got.Cmp(tt.v) != 0 {
				t.Fatalf("roundtrip mismatch: expected %v, got %v", tt.v, got)
			}
		})
	}
}

func TestAsKeyBound_InvalidPanics(t *testing.T) {
	tests := []struct {
		name string
		v    *big.Int
	}{
		{name: "negative", v: big.NewInt(-1)},
		{name: "greaterThanUint128Max", v: new(big.Int).Add(uint128Max, big.NewInt(1))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("expected panic for %s, got none", tt.name)
				}
			}()

			_ = asKeyBound(tt.v)
		})
	}
}

func TestBuildKeyRangesSplitMerge_PartitionCoverageAndOrdering(t *testing.T) {
	const partitionCount = 4

	ranges := BuildKeyRangesSplitMerge(partitionCount)
	if len(ranges) != partitionCount {
		t.Fatalf("expected %d ranges, got %d", partitionCount, len(ranges))
	}

	// First range has empty From, last range has empty To.
	if len(ranges[0].From) != 0 {
		t.Fatalf("expected first range From to be empty, got %v", ranges[0].From)
	}
	if len(ranges[partitionCount-1].To) != 0 {
		t.Fatalf("expected last range To to be empty, got %v", ranges[partitionCount-1].To)
	}

	// Check that ranges are contiguous and ordered by From/To.
	for i := range partitionCount {
		if i > 0 {
			if string(ranges[i].From) != string(ranges[i-1].To) {
				t.Fatalf(
					"expected range %d From to equal previous To, got From=%v, prevTo=%v",
					i,
					ranges[i].From,
					ranges[i-1].To,
				)
			}
		}
	}
}
