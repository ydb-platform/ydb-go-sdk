package sugar

import (
	"encoding/binary"
	"math"

	"golang.org/x/exp/constraints"

	"github.com/ydb-platform/ydb-go-sdk/v3/types"
)

type number interface {
	constraints.Integer | constraints.Float
}

func Embedding[T number](embedding ...T) types.Value {
	bytes := make([]byte, len(embedding)*4+1)
	for i, value := range embedding {
		binary.LittleEndian.PutUint32(bytes[i*4:], math.Float32bits(float32(value)))
	}
	bytes[len(bytes)-1] = 0x01

	return types.BytesValue(bytes)
}
