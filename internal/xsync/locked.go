package xsync

import "sync"

type Locked[T any] struct {
	v  T
	mu sync.RWMutex
}

func NewLocked[T any](v T) *Locked[T] {
	return &Locked[T]{
		v: v,
	}
}

func (l *Locked[T]) Get() T {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.v
}

func (l *Locked[T]) Change(f func(prev T) T) T {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.v = f(l.v)

	return l.v
}
