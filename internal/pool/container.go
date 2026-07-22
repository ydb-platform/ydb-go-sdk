package pool

import "sync"

type sliceContainer[PT ItemConstraint[T], T any] struct {
	mu   sync.Mutex
	data []*itemInfo[PT, T]
	head int
}

func (items *sliceContainer[PT, T]) PutWithCheckLimit(info *itemInfo[PT, T], limit int) error {
	items.mu.Lock()
	defer items.mu.Unlock()

	if items.len() >= limit {
		return errPoolIsOverflow
	}

	return items.put(info)
}

func (items *sliceContainer[PT, T]) Clear() (data []*itemInfo[PT, T]) {
	items.mu.Lock()
	defer items.mu.Unlock()

	defer func() {
		items.data = nil
		items.head = 0
	}()

	return items.data[items.head:]
}

func (items *sliceContainer[PT, T]) Len() int {
	items.mu.Lock()
	defer items.mu.Unlock()

	return items.len()
}

func (items *sliceContainer[PT, T]) Put(info *itemInfo[PT, T]) error {
	items.mu.Lock()
	defer items.mu.Unlock()

	return items.put(info)
}

func (items *sliceContainer[PT, T]) put(info *itemInfo[PT, T]) error {
	items.compact()
	items.data = append(items.data, info)

	return nil
}

func (items *sliceContainer[PT, T]) len() int {
	return len(items.data) - items.head
}

func (items *sliceContainer[PT, T]) compact() {
	if items.head == 0 || items.head*2 < len(items.data) {
		return
	}

	n := copy(items.data, items.data[items.head:])
	clear(items.data[n:])
	items.data = items.data[:n]
	items.head = 0
}

func (items *sliceContainer[PT, T]) resetIfEmpty() {
	if items.head == len(items.data) {
		items.data = nil
		items.head = 0
	}
}

func (items *sliceContainer[PT, T]) Pop() (info *itemInfo[PT, T], _ error) {
	items.mu.Lock()
	defer items.mu.Unlock()

	return items.pop()
}

func (items *sliceContainer[PT, T]) pop() (info *itemInfo[PT, T], _ error) {
	if items.len() == 0 {
		return nil, errNothingIdleItems
	}

	last := len(items.data) - 1
	info = items.data[last]
	items.data[last] = nil
	items.data = items.data[:last]
	items.resetIfEmpty()

	return info, nil
}

func (items *sliceContainer[PT, T]) PopOrOldestIf(
	predicate func(*itemInfo[PT, T]) bool,
) (*itemInfo[PT, T], error) {
	items.mu.Lock()
	defer items.mu.Unlock()

	if info, ok := items.popOldestIf(predicate); ok {
		return info, nil
	}

	return items.pop()
}

func (items *sliceContainer[PT, T]) popOldestIf(
	predicate func(*itemInfo[PT, T]) bool,
) (*itemInfo[PT, T], bool) {
	if items.len() == 0 || !predicate(items.data[items.head]) {
		return nil, false
	}

	info := items.data[items.head]
	items.data[items.head] = nil
	items.head++
	items.resetIfEmpty()

	return info, true
}

func (items *sliceContainer[PT, T]) PopByNodeID(nodeID uint32) (*itemInfo[PT, T], error) {
	items.mu.Lock()
	defer items.mu.Unlock()

	return items.popByNodeID(nodeID)
}

func (items *sliceContainer[PT, T]) PopByNodeIDOrOldestIf(
	nodeID uint32,
	predicate func(*itemInfo[PT, T]) bool,
) (*itemInfo[PT, T], error) {
	items.mu.Lock()
	defer items.mu.Unlock()

	if info, ok := items.popOldestIf(predicate); ok {
		return info, nil
	}

	return items.popByNodeID(nodeID)
}

func (items *sliceContainer[PT, T]) popByNodeID(nodeID uint32) (*itemInfo[PT, T], error) {
	if items.len() == 0 {
		return nil, errNothingIdleItems
	}

	for i := len(items.data) - 1; i >= items.head; i-- {
		if items.data[i].item.NodeID() != nodeID {
			continue
		}

		last := len(items.data) - 1
		info := items.data[i]
		copy(items.data[i:], items.data[i+1:])
		items.data[last] = nil
		items.data = items.data[:last]
		items.resetIfEmpty()

		return info, nil
	}

	return nil, errNothingIdleItems
}
