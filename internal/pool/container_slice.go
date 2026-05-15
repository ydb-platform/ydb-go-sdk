package pool

import (
	"sync"
)

type sliceContainer[PT ItemConstraint[T], T any] struct {
	mu   sync.Mutex
	data []*itemInfo[PT, T]
}

func (container *sliceContainer[PT, T]) PopAll() (data []*itemInfo[PT, T]) {
	container.mu.Lock()
	defer container.mu.Unlock()

	defer func() {
		container.data = nil
	}()

	return container.data
}

func (container *sliceContainer[PT, T]) Len() int {
	container.mu.Lock()
	defer container.mu.Unlock()

	return len(container.data)
}

func (container *sliceContainer[PT, T]) Put(info *itemInfo[PT, T]) error {
	container.mu.Lock()
	defer container.mu.Unlock()

	container.data = append(container.data, info)

	return nil
}

func (container *sliceContainer[PT, T]) PopAny() (info *itemInfo[PT, T], _ error) {
	container.mu.Lock()
	defer container.mu.Unlock()

	if len(container.data) == 0 {
		return nil, errNothingIdleItems
	}

	info, container.data = container.data[0], container.data[1:]

	return info, nil
}

func (container *sliceContainer[PT, T]) PopByNodeID(nodeID uint32) (*itemInfo[PT, T], error) {
	container.mu.Lock()
	defer container.mu.Unlock()

	if len(container.data) == 0 {
		return nil, errNothingIdleItems
	}

	for i, info := range container.data {
		if info.item.NodeID() == nodeID {
			container.data = append(container.data[:i], container.data[i+1:]...)

			return info, nil
		}
	}

	return nil, errNothingIdleItems
}
