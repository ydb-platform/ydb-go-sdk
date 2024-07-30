package xsync

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSet(t *testing.T) {
	var s Set[int]
	require.False(t, s.Has(1))
	require.True(t, s.Add(1))
	require.False(t, s.Add(1))
	require.True(t, s.Has(1))
	require.True(t, s.Add(2))
	require.True(t, s.Remove(1))
	require.False(t, s.Has(1))
	require.Equal(t, 1, s.Size())
	require.Equal(t, 1, s.Clear())
	require.Zero(t, s.Size())
}
