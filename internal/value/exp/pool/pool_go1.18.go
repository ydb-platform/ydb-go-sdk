//go:build go1.18
// +build go1.18

package pool

import (
	"sync"
)

type Pool[T any] sync.Pool

func (p *Pool[T]) Get() *T {
	v := (*sync.Pool)(p).Get()
	if v == nil {
		var zero T
		v = &zero
	}
	return v.(*T)
}

func (p *Pool[T]) Put(t *T) {
	(*sync.Pool)(p).Put(t)
}
