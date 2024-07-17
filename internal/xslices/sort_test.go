package xslices

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSort(t *testing.T) {
	ints := []int{1, 3, 2, 1}
	Sort(ints, func(lhs, rhs int) int {
		return lhs - rhs
	})
	require.Equal(t, []int{1, 1, 2, 3}, ints)
}
