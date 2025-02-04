package xsync

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSlice(t *testing.T) {
	var s Slice[int]
	require.Equal(t, 0, s.Size())
	require.Equal(t, 0, s.Clear())
	s.Append(1)
	require.Equal(t, 1, s.Size())
	s.Range(func(idx int, v int) bool {
		require.Equal(t, 0, idx)
		require.Equal(t, 1, v)

		return true
	})
	require.False(t, s.Remove(1))
	s.Append(2)
	var rangeFuncCounter int
	s.Range(func(idx int, v int) bool {
		rangeFuncCounter++

		return false
	})
	require.Equal(t, 1, rangeFuncCounter)
	require.True(t, s.Remove(1))
	require.Equal(t, 1, s.Clear())
}
