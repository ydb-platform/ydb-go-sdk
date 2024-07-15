package xsync

import (
	"fmt"
	"sync"
)

type Map[K comparable, V any] struct {
	m sync.Map
}

func (m *Map[K, V]) Load(key K) (value V, ok bool) {
	v, ok := m.m.Load(key)
	if !ok {
		return value, false
	}
	value, ok = v.(V)

	return value, ok
}

func (m *Map[K, V]) Must(key K) (value V) {
	v, ok := m.m.Load(key)
	if !ok {
		panic(fmt.Sprintf("unexpected key = %v", key))
	}
	value, ok = v.(V)
	if !ok {
		panic(fmt.Sprintf("unexpected type of value = %T", v))
	}

	return value
}

func (m *Map[K, V]) Has(key K) bool {
	_, ok := m.m.Load(key)

	return ok
}

func (m *Map[K, V]) Store(key K, value V) {
	m.m.Store(key, value)
}

func (m *Map[K, V]) LoadAndDelete(key K) (value V, ok bool) {
	v, ok := m.m.LoadAndDelete(key)
	if !ok {
		return value, false
	}

	value, ok = v.(V)

	return value, ok
}

func (m *Map[K, V]) Range(f func(key K, value V) bool) {
	m.m.Range(func(k, v any) bool {
		return f(k.(K), v.(V)) //nolint:forcetypeassert
	})
}

func (m *Map[K, V]) Clear() {
	m.m.Range(func(k, v any) bool {
		m.m.Delete(k)

		return true
	})
}
