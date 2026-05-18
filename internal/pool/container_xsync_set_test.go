package pool

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"

type xsyncSetContainer[PT ItemConstraint[T], T any] struct {
	data xsync.Set[*itemInfo[PT, T]]
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
		return errItemAlreadyExists
	}

	return nil
}

func (items *xsyncSetContainer[PT, T]) Pop() (info *itemInfo[PT, T], _ error) {
	items.data.Range(func(idle *itemInfo[PT, T]) bool {
		info = idle
		items.data.Remove(idle)

		return false
	})

	if info == nil {
		return nil, errNothingIdleItems
	}

	return info, nil
}

func (items *xsyncSetContainer[PT, T]) PopByNodeID(nodeID uint32) (info *itemInfo[PT, T], _ error) {
	items.data.Range(func(idle *itemInfo[PT, T]) bool {
		if idle.item.NodeID() == nodeID {
			info = idle
			items.data.Remove(idle)

			return false
		}

		return true
	})

	if info == nil {
		return nil, errNothingIdleItems
	}

	return info, nil
}
