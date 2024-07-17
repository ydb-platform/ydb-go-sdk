package xslices

import (
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
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
