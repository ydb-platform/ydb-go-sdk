package pool

import (
	"sync"
)

type mapContainer[PT ItemConstraint[T], T any] struct {
	mu   sync.Mutex
	data map[*itemInfo[PT, T]]struct{}
}

func (container *mapContainer[PT, T]) PopAll() (data []*itemInfo[PT, T]) {
	container.mu.Lock()
	defer container.mu.Unlock()

	defer func() {
		container.data = make(map[*itemInfo[PT, T]]struct{})
	}()

	for info := range container.data {
		data = append(data, info)
	}

	return data
}

func (container *mapContainer[PT, T]) Len() int {
	container.mu.Lock()
	defer container.mu.Unlock()

	return len(container.data)
}

func (container *mapContainer[PT, T]) Put(info *itemInfo[PT, T]) error {
	container.mu.Lock()
	defer container.mu.Unlock()

	if container.data == nil {
		container.data = make(map[*itemInfo[PT, T]]struct{})
	}

	container.data[info] = struct{}{}

	return nil
}

func (container *mapContainer[PT, T]) Pop() (info *itemInfo[PT, T], _ error) {
	container.mu.Lock()
	defer container.mu.Unlock()

	if container.data == nil {
		container.data = make(map[*itemInfo[PT, T]]struct{})
	}

	for info := range container.data {
		delete(container.data, info)

		return info, nil
	}

	return nil, errNothingIdleItems
}

func (container *mapContainer[PT, T]) PopByNodeID(nodeID uint32) (*itemInfo[PT, T], error) {
	container.mu.Lock()
	defer container.mu.Unlock()

	if container.data == nil {
		container.data = make(map[*itemInfo[PT, T]]struct{})
	}

	for info := range container.data {
		if info.item.NodeID() == nodeID {
			delete(container.data, info)

			return info, nil
		}
	}

	return nil, errNothingIdleItems
}
