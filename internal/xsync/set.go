package xsync

import (
	"sync"
)

type Set[K comparable] struct {
	m sync.Map
}

func (m *Set[K]) Has(key K) bool {
	_, ok := m.m.Load(key)

	return ok
}

func (m *Set[K]) Add(key K) bool {
	_, loaded := m.m.LoadOrStore(key, struct{}{})

	return !loaded
}

func (m *Set[K]) Remove(key K) (ok bool) {
	_, ok = m.m.LoadAndDelete(key)

	return ok
}

func (m *Set[K]) Clear() (removed int) {
	m.m.Range(func(k, v any) bool {
		removed++

		m.m.Delete(k)

		return true
	})

	return removed
}
