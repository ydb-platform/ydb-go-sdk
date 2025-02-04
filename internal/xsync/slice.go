package xsync

import (
	"sync"
)

type Slice[T any] struct {
	m sync.RWMutex
	s []T
}

func (s *Slice[T]) Append(v T) {
	s.m.Lock()
	defer s.m.Unlock()

	s.s = append(s.s, v)
}

func (s *Slice[T]) Size() int {
	s.m.RLock()
	defer s.m.RUnlock()

	return len(s.s)
}

func (s *Slice[T]) Range(f func(idx int, v T) bool) {
	s.m.RLock()
	defer s.m.RUnlock()

	for idx, v := range s.s {
		if !f(idx, v) {
			break
		}
	}
}

func (s *Slice[T]) Remove(idx int) bool {
	s.m.Lock()
	defer s.m.Unlock()

	if idx >= len(s.s) {
		return false
	}

	s.s = append(s.s[:idx], s.s[idx+1:]...)

	return true
}

func (s *Slice[T]) Clear() (removed int) {
	s.m.Lock()
	defer s.m.Unlock()

	if s.s == nil {
		return 0
	}

	l := len(s.s)
	s.s = s.s[:0]

	return l
}
