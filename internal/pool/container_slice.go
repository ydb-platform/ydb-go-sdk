package pool

import (
	"sync"
)

type sliceContainer[PT ItemConstraint[T], T any] struct {
	mu   sync.Mutex
	data []*itemInfo[PT, T]
}

func (items *sliceContainer[PT, T]) PutWithCheckLimit(info *itemInfo[PT, T], limit int) error {
	items.mu.Lock()
	defer items.mu.Unlock()

	if len(items.data) >= limit {
		return errPoolIsOverflow
	}

	return items.put(info)
}

func (items *sliceContainer[PT, T]) Clear() (data []*itemInfo[PT, T]) {
	items.mu.Lock()
	defer items.mu.Unlock()

	defer func() {
		items.data = nil
	}()

	return items.data
}

func (items *sliceContainer[PT, T]) Len() int {
	items.mu.Lock()
	defer items.mu.Unlock()

	return len(items.data)
}

func (items *sliceContainer[PT, T]) Put(info *itemInfo[PT, T]) error {
	items.mu.Lock()
	defer items.mu.Unlock()

	return items.put(info)
}

func (items *sliceContainer[PT, T]) put(info *itemInfo[PT, T]) error {
	items.data = append(items.data, info)

	return nil
}

func (items *sliceContainer[PT, T]) Pop() (info *itemInfo[PT, T], _ error) {
	items.mu.Lock()
	defer items.mu.Unlock()

	if len(items.data) == 0 {
		return nil, errNothingIdleItems
	}

	info, items.data = items.data[len(items.data)-1], items.data[:len(items.data)-1]

	return info, nil
}

func (items *sliceContainer[PT, T]) PopByNodeID(nodeID uint32) (*itemInfo[PT, T], error) {
	items.mu.Lock()
	defer items.mu.Unlock()

	if len(items.data) == 0 {
		return nil, errNothingIdleItems
	}

	for i := len(items.data) - 1; i >= 0; i-- {
		info := items.data[i]
		if info.item.NodeID() == nodeID {
			items.data = append(items.data[:i], items.data[i+1:]...)

			return info, nil
		}
	}

	return nil, errNothingIdleItems
}
