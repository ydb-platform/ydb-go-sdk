package xslices

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransform(t *testing.T) {
	require.Equal(t, []string{"1", "2", "3"}, Transform([]int{1, 2, 3}, strconv.Itoa))
}
