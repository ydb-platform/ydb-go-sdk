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
