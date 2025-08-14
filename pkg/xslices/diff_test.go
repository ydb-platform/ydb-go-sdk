package xslices

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDiff(t *testing.T) {
	for _, tt := range []struct {
		name     string
		previous []int
		newest   []int
		steady   []int
		added    []int
		dropped  []int
	}{
		{
			name: "WithoutChanges",
			previous: []int{
				2,
				1,
				0,
				3,
			},
			newest: []int{
				1,
				3,
				2,
				0,
			},
			steady: []int{
				0,
				1,
				2,
				3,
			},
			added:   []int{},
			dropped: []int{},
		},
		{
			name: "OnlyAdded",
			previous: []int{
				1,
				0,
				3,
			},
			newest: []int{
				1,
				3,
				2,
				0,
			},
			steady: []int{
				0,
				1,
				3,
			},
			added: []int{
				2,
			},
			dropped: []int{},
		},
		{
			name: "OnlyAddedWithDuplicatesInPrevious",
			previous: []int{
				1,
				0,
				1,
				3,
			},
			newest: []int{
				1,
				3,
				2,
				0,
			},
			steady: []int{
				0,
				1,
				3,
			},
			added: []int{
				2,
			},
			dropped: []int{
				1,
			},
		},
		{
			name: "OnlyAddedWithDuplicatesInNewest",
			previous: []int{
				1,
				0,
				3,
			},
			newest: []int{
				1,
				1,
				3,
				1,
				2,
				0,
			},
			steady: []int{
				0,
				1,
				3,
			},
			added: []int{
				1,
				1,
				2,
			},
			dropped: []int{},
		},
		{
			name: "OnlyDropped",
			previous: []int{
				1,
				2,
				0,
				3,
			},
			newest: []int{
				1,
				3,
				0,
			},
			steady: []int{
				0,
				1,
				3,
			},
			added: []int{},
			dropped: []int{
				2,
			},
		},
		{
			name: "AddedAndDropped1",
			previous: []int{
				4,
				7,
				8,
			},
			newest: []int{
				1,
				3,
				0,
			},
			steady: []int{},
			added: []int{
				0,
				1,
				3,
			},
			dropped: []int{
				4,
				7,
				8,
			},
		},
		{
			name: "AddedAndDropped2",
			previous: []int{
				1,
				3,
				0,
			},
			newest: []int{
				4,
				7,
				8,
			},
			steady: []int{},
			added: []int{
				4,
				7,
				8,
			},
			dropped: []int{
				0,
				1,
				3,
			},
		},
		{
			name: "AddedWithDuplicates",
			previous: []int{
				1,
				3,
				3,
				0,
			},
			newest: []int{
				4,
				7,
				7,
				8,
			},
			steady: []int{},
			added: []int{
				4,
				7,
				7,
				8,
			},
			dropped: []int{
				0,
				1,
				3,
				3,
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			steady, added, dropped := Diff(tt.previous, tt.newest, func(lhs, rhs int) int {
				return lhs - rhs
			})
			require.Equal(t, tt.added, added)
			require.Equal(t, tt.dropped, dropped)
			require.Equal(t, tt.steady, steady)
		})
	}
}
