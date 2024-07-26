package xslices

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSplit(t *testing.T) {
	good, bad := Split([]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, func(t int) bool {
		return t%2 == 0
	})
	require.Equal(t, []int{0, 2, 4, 6, 8}, good)
	require.Equal(t, []int{1, 3, 5, 7, 9}, bad)
}
