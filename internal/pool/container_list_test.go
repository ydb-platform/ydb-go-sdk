package pool

import (
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xlist"
)

type listContainer[PT ItemConstraint[T], T any] struct {
	mu   sync.Mutex
	data xlist.List[*itemInfo[PT, T]]
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
