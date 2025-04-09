package allocator

type Allocator struct{}

func New() *Allocator {
	return &Allocator{}
}

func (a *Allocator) Free() {}
