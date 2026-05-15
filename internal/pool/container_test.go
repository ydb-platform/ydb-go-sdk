package pool

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const containerLen = 500

func BenchmarkContainers(b *testing.B) {
	for _, tt := range []struct {
		name      string
		container container[*testItem, testItem]
	}{
		{
			name:      "xsync.Set",
			container: &xsyncSetContainer[*testItem, testItem]{},
		},
		{
			name:      "slice",
			container: &sliceContainer[*testItem, testItem]{},
		},
		{
			name:      "map",
			container: &mapContainer[*testItem, testItem]{},
		},
		{
			name:      "xlist.List",
			container: &listContainer[*testItem, testItem]{},
		},
	} {
		b.Run(tt.name, func(b *testing.B) {
			container := tt.container
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
				info, err := container.PopAny()
				require.NoError(b, err)
				container.Put(info)
				info, err = container.PopByNodeID(i % containerLen)
				require.NoError(b, err)
				container.Put(info)
			}

			require.Equal(b, containerLen, container.Len())
			data := container.PopAll()
			require.Len(b, data, containerLen)
		})
	}
}
