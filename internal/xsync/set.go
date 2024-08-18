package xsync

import (
	"sync"
	"sync/atomic"
)

type Set[K comparable] struct {
	m    sync.Map
	size atomic.Int32
}

func (s *Set[K]) Has(key K) bool {
	_, ok := s.m.Load(key)

	return ok
}

func (s *Set[K]) Add(key K) bool {
	_, exists := s.m.LoadOrStore(key, struct{}{})

	if !exists {
		s.size.Add(1)
	}

	return !exists
}

func (s *Set[K]) Size() int {
	return int(s.size.Load())
}

func (s *Set[K]) Range(f func(key K) bool) {
	s.m.Range(func(k, v any) bool {
		return f(k.(K)) //nolint:forcetypeassert
	})
}

func (s *Set[K]) Remove(key K) bool {
	_, exists := s.m.LoadAndDelete(key)

	if exists {
		s.size.Add(-1)
	}

	return exists
}

func (s *Set[K]) Clear() (removed int) {
	s.m.Range(func(k, v any) bool {
		removed++

		s.m.Delete(k)

		return true
	})
	s.size.Add(int32(-removed))

	return removed
}
