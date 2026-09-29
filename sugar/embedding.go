package sugar

import (
	"encoding/binary"
	"math"

	"github.com/ydb-platform/ydb-go-sdk/v3/types"
)

type number interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr |
		~float32 | ~float64
}

// Embedding returns a Bytes value containing the given numbers as float32 values.
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func Embedding[T number](embedding ...T) types.Value {
	bytes := make([]byte, len(embedding)*4+1)
	for i, value := range embedding {
		binary.LittleEndian.PutUint32(bytes[i*4:], math.Float32bits(float32(value)))
	}
	bytes[len(bytes)-1] = 0x01

	return types.BytesValue(bytes)
}
