package sugar

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/types"
)

func TestEmbedding(t *testing.T) {
	value := Embedding(1, -2, 0.5)
	require.Equal(t, types.BytesValue([]byte{
		0x00, 0x00, 0x80, 0x3f, 0x00, 0x00, 0x00,
		0xc0, 0x00, 0x00, 0x00, 0x3f, 0x01,
	}), value)
}

func TestEmbeddingInteger(t *testing.T) {
	require.Equal(t, types.BytesValue([]byte{
		0x00, 0x00, 0x80, 0x4b, 0x01,
	}), Embedding(int64(16777217)))
	require.Equal(t, types.BytesValue([]byte{
		0x00, 0x00, 0x00, 0xc0, 0x01,
	}), Embedding(int16(-2)))
}

func TestEmbeddingByteTypes(t *testing.T) {
	require.Equal(t, types.BytesValue([]byte{0xff, 0x02, 0x03}), Embedding(int8(-1), int8(2)))
	require.Equal(t, types.BytesValue([]byte{0x00, 0xff, 0x02}), Embedding(uint8(0), uint8(255)))

	type signedByte int8
	type unsignedByte uint8
	require.Equal(t, types.BytesValue([]byte{0xff, 0x03}), Embedding(signedByte(-1)))
	require.Equal(t, types.BytesValue([]byte{0xff, 0x02}), Embedding(unsignedByte(255)))
}

func TestEmbeddingFloat64Precision(t *testing.T) {
	require.Equal(t, types.BytesValue([]byte{
		0x00, 0x00, 0x80, 0x3f, 0x01,
	}), Embedding(float64(1.00000001)))
}

func TestEmbeddingEmpty(t *testing.T) {
	require.Equal(t, types.BytesValue([]byte{0x01}), Embedding[float32]())
	require.Equal(t, types.BytesValue([]byte{0x02}), Embedding[uint8]())
	require.Equal(t, types.BytesValue([]byte{0x03}), Embedding[int8]())
}
