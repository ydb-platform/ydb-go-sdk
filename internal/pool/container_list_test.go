package pool

import (
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xlist"
)

type listContainer[PT ItemConstraint[T], T any] struct {
	mu   sync.Mutex
	data xlist.List[*itemInfo[PT, T]]
}

func (container *listContainer[PT, T]) PopAll() (data []*itemInfo[PT, T]) {
	container.mu.Lock()
	defer container.mu.Unlock()

	if container.data == nil {
		container.data = xlist.New[*itemInfo[PT, T]]()
	}

	for iter := container.data.Front(); iter != nil; iter = iter.Next() {
		data = append(data, iter.Value)
	}

	container.data.Clear()

	return data
}

func (container *listContainer[PT, T]) Len() int {
	container.mu.Lock()
	defer container.mu.Unlock()

	if container.data == nil {
		container.data = xlist.New[*itemInfo[PT, T]]()
	}

	return container.data.Len()
}

func (container *listContainer[PT, T]) Put(info *itemInfo[PT, T]) error {
	container.mu.Lock()
	defer container.mu.Unlock()

	if container.data == nil {
		container.data = xlist.New[*itemInfo[PT, T]]()
	}

	container.data.PushBack(info)

	return nil
}

func (container *listContainer[PT, T]) Pop() (info *itemInfo[PT, T], _ error) {
	container.mu.Lock()
	defer container.mu.Unlock()

	if container.data == nil {
		container.data = xlist.New[*itemInfo[PT, T]]()
	}

	iter := container.data.Front()
	if iter != nil {
		container.data.Remove(iter)

		return iter.Value, nil
	}

	return nil, errNothingIdleItems
}

func (container *listContainer[PT, T]) PopByNodeID(nodeID uint32) (*itemInfo[PT, T], error) {
	container.mu.Lock()
	defer container.mu.Unlock()

	if container.data == nil {
		container.data = xlist.New[*itemInfo[PT, T]]()
	}

	for iter := container.data.Front(); iter != nil; iter = iter.Next() {
		if iter.Value.item.NodeID() == nodeID {
			container.data.Remove(iter)

			return iter.Value, nil
		}
	}

	return nil, errNothingIdleItems
}
