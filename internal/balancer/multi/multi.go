package multi

import (
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

func Balancer(opts ...Option) balancer.Balancer {
	m := new(multi)
	for _, opt := range opts {
		opt(m)
	}
	return m
}

type multiHandle struct {
	elements []balancer.Element
}

type multi struct {
	mu       sync.RWMutex
	balancer []balancer.Balancer
	filter   []func(conn.Conn) bool
}

func (m *multi) Create() balancer.Balancer {
	bb := make([]balancer.Balancer, len(m.balancer))
	for i, b := range m.balancer {
		bb[i] = b.Create()
	}
	return &multi{
		balancer: bb,
		filter:   m.filter,
	}
}

func WithBalancer(b balancer.Balancer, filter func(cc conn.Conn) bool) Option {
	return func(m *multi) {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.balancer = append(m.balancer, b)
		m.filter = append(m.filter, filter)
	}
}

type Option func(*multi)

func (m *multi) Contains(x balancer.Element) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for i, h := range x.(multiHandle).elements {
		if h != nil && m.balancer[i].Contains(h) {
			return true
		}
	}
	return false
}

func (m *multi) Next() conn.Conn {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, b := range m.balancer {
		if c := b.Next(); c != nil {
			return c
		}
	}
	return nil
}

func (m *multi) Insert(conn conn.Conn) balancer.Element {
	m.mu.Lock()
	defer m.mu.Unlock()

	var (
		n = len(m.filter)
		h = multiHandle{
			elements: make([]balancer.Element, n),
		}
		inserted = false
	)

	for i, f := range m.filter {
		if f(conn) {
			h.elements[i] = m.balancer[i].Insert(conn)
			inserted = true
		}
	}
	if inserted {
		return h
	}
	return nil
}

func (m *multi) Remove(x balancer.Element) (removed bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i, h := range x.(multiHandle).elements {
		if h != nil {
			if m.balancer[i].Remove(h) {
				removed = true
			}
		}
	}
	return removed
}
