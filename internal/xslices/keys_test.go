package xslices

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKeys(t *testing.T) {
	require.Equal(t, []string{"1", "2"}, Keys(map[string]any{
		"1": nil,
		"2": nil,
	}))
	require.Equal(t, []int{1, 2}, Keys(map[int]any{
		1: nil,
		2: nil,
	}))
}
