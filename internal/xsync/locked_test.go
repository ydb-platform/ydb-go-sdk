package xsync

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLocked(t *testing.T) {
	l := NewLocked[int](1)
	require.Equal(t, 1, l.Get())
	require.Equal(t, 2, l.Change(func(v int) int {
		return 2
	}))
	require.Equal(t, 2, l.Get())
}
