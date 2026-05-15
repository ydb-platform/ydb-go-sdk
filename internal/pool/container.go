package pool

type container[PT ItemConstraint[T], T any] interface {
	Len() int
	Put(info *itemInfo[PT, T]) error
	PopAny() (*itemInfo[PT, T], error)
	PopByNodeID(nodeID uint32) (*itemInfo[PT, T], error)
	PopAll() []*itemInfo[PT, T]
}
