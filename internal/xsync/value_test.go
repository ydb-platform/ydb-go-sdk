package xsync

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValue(t *testing.T) {
	v := NewValue(5)
	require.Equal(t, 5, v.Get())
	v.Change(func(old int) int {
		return 6
	})
	require.Equal(t, 6, v.Get())
}
