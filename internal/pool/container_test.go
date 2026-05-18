package pool

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const containerLen = 500

// BenchmarkContainers/xsync.Set-12         	  747793	      1585 ns/op	      96 B/op	       2 allocs/op
// BenchmarkContainers/slice-12             	 3094249	      390.7 ns/op	      0 B/op	       0 allocs/op
// BenchmarkContainers/map-12               	  511998	      2247 ns/op	      0 B/op	       0 allocs/op
// BenchmarkContainers/xlist.List-12        	  921478	      1274 ns/op	      64 B/op	       2 allocs/op
func BenchmarkContainers(b *testing.B) {
	for _, tt := range []struct {
		name  string
		items itemsContainer[*testItem, testItem]
	}{
		{
			name:  "xsync.Set",
			items: &xsyncSetContainer[*testItem, testItem]{},
		},
		{
			name:  "slice",
			items: &sliceContainer[*testItem, testItem]{},
		},
		{
			name:  "map",
			items: &mapContainer[*testItem, testItem]{},
		},
		{
			name:  "xlist.List",
			items: &listContainer[*testItem, testItem]{},
		},
	} {
		b.Run(tt.name, func(b *testing.B) {
			container := tt.items
			for i := range containerLen {
				require.NoError(b, container.Put(&itemInfo[*testItem, testItem]{
					item: &testItem{
						v:         int32(i),
						closed:    false,
						onClose:   nil,
						onIsAlive: nil,
						onNodeID: func() uint32 {
							return uint32(i)
						},
					},
				}))
			}

			b.ResetTimer()
			b.ReportAllocs()

			require.Equal(b, containerLen, container.Len())

			var i uint32
			for b.Loop() {
				info, err := container.Pop()
				require.NoError(b, err)
				require.NoError(b, container.Put(info))
				info, err = container.PopByNodeID(i % containerLen)
				require.NoError(b, err)
				require.NoError(b, container.Put(info))
			}

			require.Equal(b, containerLen, container.Len())
			data := container.Clear()
			require.Len(b, data, containerLen)
		})
	}
}
