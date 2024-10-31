package xslices

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUniq(t *testing.T) {
	require.Equal(t, []int{1, 2, 3}, Uniq([]int{3, 2, 1, 2, 2, 3, 3}))
}
