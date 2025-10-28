package coordination

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewProtectionKey(t *testing.T) {
	key1 := newProtectionKey()
	require.NotNil(t, key1)
	require.Len(t, key1, 8)

	key2 := newProtectionKey()
	require.NotNil(t, key2)
	require.Len(t, key2, 8)

	// Protection keys should be different (with very high probability)
	require.NotEqual(t, key1, key2)
}

func TestNewReqID(t *testing.T) {
	id1 := newReqID()
	id2 := newReqID()

	// IDs should be different (with very high probability)
	require.NotEqual(t, id1, id2)
}
