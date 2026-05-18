package pool

type itemsContainer[PT ItemConstraint[T], T any] interface {
	Len() int
	Put(info *itemInfo[PT, T]) error
	PutWithCheckLimit(info *itemInfo[PT, T], limit int) error
	Pop() (*itemInfo[PT, T], error)
	PopByNodeID(nodeID uint32) (*itemInfo[PT, T], error)
	Clear() []*itemInfo[PT, T]
}
