package pool

import (
	"sync"
)

type mapContainer[PT ItemConstraint[T], T any] struct {
	mu   sync.Mutex
	data map[*itemInfo[PT, T]]struct{}
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
