// This file compare idle-queue container implementations.
//
// itemsContainer defines the interface under test;
// map, list, xsync.Set, and slice backends are exercised by BenchmarkContainers.
// sliceContainer won that benchmark (see results in BenchmarkContainers) and
// is the production implementation in container.go, wired as Pool.idle (pool.go).
package pool

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xlist"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

type (
	itemsContainer[PT ItemConstraint[T], T any] interface {
		Len() int
		Put(info *itemInfo[PT, T]) error
		PutWithCheckLimit(info *itemInfo[PT, T], limit int) error
		Pop() (*itemInfo[PT, T], error)
		PopByNodeID(nodeID uint32) (*itemInfo[PT, T], error)
		Clear() []*itemInfo[PT, T]
	}
	mapContainer[PT ItemConstraint[T], T any] struct {
		mu   sync.Mutex
		data map[*itemInfo[PT, T]]struct{}
	}
	listContainer[PT ItemConstraint[T], T any] struct {
		mu   sync.Mutex
		data xlist.List[*itemInfo[PT, T]]
	}
	xsyncSetContainer[PT ItemConstraint[T], T any] struct {
		data xsync.Set[*itemInfo[PT, T]]
	}
)

const containerLen = 500

// BenchmarkContainers/xsync.Set-12         	 1174489	      1011 ns/op	      86 B/op	       1 allocs/op
// BenchmarkContainers/slice-12             	 1000000	      1511 ns/op	       0 B/op	       0 allocs/op
// BenchmarkContainers/map-12               	  674767	      1757 ns/op	       1 B/op	       0 allocs/op
// BenchmarkContainers/xlist.List-12        	  694712	      1504 ns/op	      33 B/op	       1 allocs/op
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

			b.SetParallelism(containerLen)
			b.RunParallel(func(pb *testing.PB) {
				var i uint32
				for pb.Next() {
					{
						info, err := container.Pop()
						if err != nil {
							require.Nil(b, info)
							require.ErrorIs(b, err, errNothingIdleItems)
						} else {
							require.NoError(b, container.Put(info))
						}
					}
					{
						info, err := container.PopByNodeID(i % containerLen)
						if err != nil {
							require.Nil(b, info)
							require.ErrorIs(b, err, errNothingIdleItems)
						} else {
							require.NoError(b, container.Put(info))
						}
					}
				}
			})

			require.Equal(b, containerLen, container.Len())
			data := container.Clear()
			require.Len(b, data, containerLen)
		})
	}
}

func (items *mapContainer[PT, T]) PutWithCheckLimit(info *itemInfo[PT, T], limit int) error {
	items.mu.Lock()
	defer items.mu.Unlock()

	if len(items.data) >= limit {
		return errPoolIsOverflow
	}

	return items.put(info)
}

func (items *mapContainer[PT, T]) Clear() (data []*itemInfo[PT, T]) {
	items.mu.Lock()
	defer items.mu.Unlock()

	defer func() {
		items.data = make(map[*itemInfo[PT, T]]struct{})
	}()

	for info := range items.data {
		data = append(data, info)
	}

	return data
}

func (items *mapContainer[PT, T]) Len() int {
	items.mu.Lock()
	defer items.mu.Unlock()

	return len(items.data)
}

func (items *mapContainer[PT, T]) Put(info *itemInfo[PT, T]) error {
	items.mu.Lock()
	defer items.mu.Unlock()

	return items.put(info)
}

func (items *mapContainer[PT, T]) put(info *itemInfo[PT, T]) error {
	if items.data == nil {
		items.data = make(map[*itemInfo[PT, T]]struct{})
	}

	items.data[info] = struct{}{}

	return nil
}

func (items *mapContainer[PT, T]) Pop() (info *itemInfo[PT, T], _ error) {
	items.mu.Lock()
	defer items.mu.Unlock()

	if items.data == nil {
		items.data = make(map[*itemInfo[PT, T]]struct{})
	}

	for info := range items.data {
		delete(items.data, info)

		return info, nil
	}

	return nil, errNothingIdleItems
}

func (items *mapContainer[PT, T]) PopByNodeID(nodeID uint32) (*itemInfo[PT, T], error) {
	items.mu.Lock()
	defer items.mu.Unlock()

	if items.data == nil {
		items.data = make(map[*itemInfo[PT, T]]struct{})
	}

	for info := range items.data {
		if info.item.NodeID() == nodeID {
			delete(items.data, info)

			return info, nil
		}
	}

	return nil, errNothingIdleItems
}

func (items *listContainer[PT, T]) PutWithCheckLimit(info *itemInfo[PT, T], limit int) error {
	items.mu.Lock()
	defer items.mu.Unlock()

	if items.data.Len() >= limit {
		return errPoolIsOverflow
	}

	return items.put(info)
}

func (items *listContainer[PT, T]) Clear() (data []*itemInfo[PT, T]) {
	items.mu.Lock()
	defer items.mu.Unlock()

	if items.data == nil {
		items.data = xlist.New[*itemInfo[PT, T]]()
	}

	for iter := items.data.Front(); iter != nil; iter = iter.Next() {
		data = append(data, iter.Value)
	}

	items.data.Clear()

	return data
}

func (items *listContainer[PT, T]) Len() int {
	items.mu.Lock()
	defer items.mu.Unlock()

	if items.data == nil {
		items.data = xlist.New[*itemInfo[PT, T]]()
	}

	return items.data.Len()
}

func (items *listContainer[PT, T]) Put(info *itemInfo[PT, T]) error {
	items.mu.Lock()
	defer items.mu.Unlock()

	return items.put(info)
}

func (items *listContainer[PT, T]) put(info *itemInfo[PT, T]) error {
	if items.data == nil {
		items.data = xlist.New[*itemInfo[PT, T]]()
	}

	items.data.PushBack(info)

	return nil
}

func (items *listContainer[PT, T]) Pop() (info *itemInfo[PT, T], _ error) {
	items.mu.Lock()
	defer items.mu.Unlock()

	if items.data == nil {
		items.data = xlist.New[*itemInfo[PT, T]]()
	}

	iter := items.data.Front()
	if iter != nil {
		items.data.Remove(iter)

		return iter.Value, nil
	}

	return nil, errNothingIdleItems
}

func (items *listContainer[PT, T]) PopByNodeID(nodeID uint32) (*itemInfo[PT, T], error) {
	items.mu.Lock()
	defer items.mu.Unlock()

	if items.data == nil {
		items.data = xlist.New[*itemInfo[PT, T]]()
	}

	for iter := items.data.Front(); iter != nil; iter = iter.Next() {
		if iter.Value.item.NodeID() == nodeID {
			items.data.Remove(iter)

			return iter.Value, nil
		}
	}

	return nil, errNothingIdleItems
}

func (items *xsyncSetContainer[PT, T]) PutWithCheckLimit(info *itemInfo[PT, T], limit int) error {
	if items.data.Size() >= limit {
		return errPoolIsOverflow
	}

	return items.Put(info)
}

func (items *xsyncSetContainer[PT, T]) Clear() (data []*itemInfo[PT, T]) {
	items.data.Range(func(idle *itemInfo[PT, T]) bool {
		data = append(data, idle)
		items.data.Remove(idle)

		return true
	})

	return data
}

func (items *xsyncSetContainer[PT, T]) Len() int {
	return items.data.Size()
}

func (items *xsyncSetContainer[PT, T]) Put(info *itemInfo[PT, T]) error {
	if !items.data.Add(info) {
		return fmt.Errorf("item %+v already exists: %w", info, errItemAlreadyExists)
	}

	return nil
}

func (items *xsyncSetContainer[PT, T]) Pop() (info *itemInfo[PT, T], _ error) {
	items.data.Range(func(idle *itemInfo[PT, T]) bool {
		if items.data.Remove(idle) {
			info = idle

			return false
		}

		return true
	})

	if info == nil {
		return nil, errNothingIdleItems
	}

	return info, nil
}

func (items *xsyncSetContainer[PT, T]) PopByNodeID(nodeID uint32) (info *itemInfo[PT, T], _ error) {
	items.data.Range(func(idle *itemInfo[PT, T]) bool {
		if idle.item.NodeID() == nodeID {
			if items.data.Remove(idle) {
				info = idle

				return false
			}

			return true
		}

		return true
	})

	if info == nil {
		return nil, errNothingIdleItems
	}

	return info, nil
}
