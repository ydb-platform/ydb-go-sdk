package xslices

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFilter(t *testing.T) {
	filtered := Filter([]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, func(t int) bool {
		return t%2 == 0
	})
	require.Equal(t, []int{0, 2, 4, 6, 8}, filtered)
}
