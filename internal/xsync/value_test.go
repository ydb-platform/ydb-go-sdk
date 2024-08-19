package xsync

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValue_Change(t *testing.T) {
	v := NewValue(5)
	require.Equal(t, 5, v.Load())
	v.Change(func(old int) int {
		return 6
	})
	require.Equal(t, 6, v.Load())
}
