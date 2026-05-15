package pool

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"

type xsyncSetContainer[PT ItemConstraint[T], T any] struct {
	data xsync.Set[*itemInfo[PT, T]]
}

func (container *xsyncSetContainer[PT, T]) PopAll() (data []*itemInfo[PT, T]) {
	container.data.Range(func(idle *itemInfo[PT, T]) bool {
		data = append(data, idle)
		container.data.Remove(idle)

		return true
	})

	return data
}

func (container *xsyncSetContainer[PT, T]) Len() int {
	return container.data.Size()
}

func (container *xsyncSetContainer[PT, T]) Put(info *itemInfo[PT, T]) error {
	if !container.data.Add(info) {
		return errItemAlreadyExists
	}

	return nil
}

func (container *xsyncSetContainer[PT, T]) PopAny() (info *itemInfo[PT, T], _ error) {
	container.data.Range(func(idle *itemInfo[PT, T]) bool {
		info = idle
		container.data.Remove(idle)

		return false
	})

	if info == nil {
		return nil, errNothingIdleItems
	}

	return info, nil
}

func (container *xsyncSetContainer[PT, T]) PopByNodeID(nodeID uint32) (info *itemInfo[PT, T], _ error) {
	container.data.Range(func(idle *itemInfo[PT, T]) bool {
		if idle.item.NodeID() == nodeID {
			info = idle
			container.data.Remove(idle)

			return false
		}

		return true
	})

	if info == nil {
		return nil, errNothingIdleItems
	}

	return info, nil
}
