package xslices

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMap(t *testing.T) {
	require.Equal(t,
		map[string]int{
			"1": 1,
			"2": 2,
			"3": 3,
		},
		Map([]int{1, 2, 3}, strconv.Itoa),
	)
}
