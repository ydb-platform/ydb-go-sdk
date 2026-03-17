package xslices

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSubtract(t *testing.T) {
	for i, tt := range []struct {
		source     []int
		toSubtract []int
		difference []int
	}{
		{
			source:     []int{1, 2, 3},
			toSubtract: []int{2},
			difference: []int{1, 3},
		},
		{
			source:     []int{3, 2, 1},
			toSubtract: []int{2},
			difference: []int{3, 1},
		},
		{
			source:     []int{1, 2, 3},
			toSubtract: []int{2, 3, 4},
			difference: []int{1},
		},
		{
			source:     []int{1, 2, 3},
			toSubtract: []int{4, 5, 6},
			difference: []int{1, 2, 3},
		},
		{
			source:     []int{1, 2, 3, 2, 1},
			toSubtract: []int{1, 3},
			difference: []int{2, 2},
		},
		{
			source:     []int{1, 2, 3, 2, 1},
			toSubtract: []int{2},
			difference: []int{1, 3, 1},
		},
		{
			source:     []int{2, 2, 2, 2, 2},
			toSubtract: []int{2},
			difference: []int{},
		},
	} {
		t.Run(fmt.Sprintf("case %d", i), func(t *testing.T) {
			require.Equal(t, tt.difference, Subtract(tt.source, tt.toSubtract))
		})
	}
}
