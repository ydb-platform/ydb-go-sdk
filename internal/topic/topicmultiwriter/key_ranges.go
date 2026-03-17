package topicmultiwriter

import (
	"math/big"
)

// This code can be removed when server assigns bounds to partitions
// even when autopartitioning is not enabled in the topic.

type KeyRange struct {
	From []byte // inclusive
	To   []byte // exclusive
}

// uint128Max = 2^128-1
var uint128Max = new(big.Int).Sub(
	new(big.Int).Lsh(big.NewInt(1), 128),
	big.NewInt(1),
)

func rangeFromShardNumber(shardNumber, shardCount int) (left, right *big.Int) {
	if shardNumber < 0 || shardNumber >= shardCount {
		panic("shardNumber out of range")
	}

	if shardCount == 1 {
		return big.NewInt(0), new(big.Int).Set(uint128Max)
	}

	slice := new(big.Int).Div(uint128Max, big.NewInt(int64(shardCount)))

	left = new(big.Int).Mul(big.NewInt(int64(shardNumber)), slice)

	if shardNumber+1 == shardCount {
		right = new(big.Int).Set(uint128Max)
	} else {
		right = new(big.Int).Mul(big.NewInt(int64(shardNumber+1)), slice)
		right.Sub(right, big.NewInt(1))
	}

	return left, right
}

func asKeyBound(v *big.Int) []byte {
	if v.Sign() < 0 || v.Cmp(uint128Max) > 0 {
		panic("value out of uint128 range")
	}

	b := v.Bytes()
	if len(b) > 16 {
		panic("value does not fit into 16 bytes")
	}
	if len(b) == 16 {
		return b
	}

	res := make([]byte, 16)
	copy(res[16-len(b):], b)

	return res
}

func BuildKeyRangesSplitMerge(partitionCount int) []KeyRange {
	ranges := make([]KeyRange, partitionCount)
	var prevBound []byte

	for i := range partitionCount {
		var kr KeyRange

		if i > 0 {
			kr.From = prevBound
		}

		if i != partitionCount-1 {
			_, end := rangeFromShardNumber(i, partitionCount)
			toBound := asKeyBound(end)

			kr.To = toBound
			prevBound = toBound
		}

		ranges[i] = kr
	}

	return ranges
}
