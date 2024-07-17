package xslices

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSortCopy(t *testing.T) {
	src := []int{3, 2, 1}
	dst := SortCopy(src, func(lhs, rhs int) int {
		return lhs - rhs
	})
	require.Equal(t, len(src), len(dst))
	require.NotEqual(t, src, dst)
	require.Equal(t, []int{3, 2, 1}, src)
	require.Equal(t, []int{1, 2, 3}, dst)
}
