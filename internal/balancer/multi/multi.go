package multi

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint/info"
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
		m.balancer = append(m.balancer, b)
		m.filter = append(m.filter, filter)
	}
}

type Option func(*multi)

func (m *multi) Contains(x balancer.Element) bool {
	for i, h := range x.(multiHandle).elements {
		if h != nil && m.balancer[i].Contains(h) {
			return true
		}
	}
	return false
}

func (m *multi) Next() conn.Conn {
	for _, b := range m.balancer {
		if c := b.Next(); c != nil {
			return c
		}
	}
	return nil
}

func (m *multi) Insert(conn conn.Conn) balancer.Element {
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

func (m *multi) Update(x balancer.Element, info info.Info) {
	for i, h := range x.(multiHandle).elements {
		if h != nil {
			m.balancer[i].Update(h, info)
		}
	}
}

func (m *multi) Remove(x balancer.Element) (removed bool) {
	for i, h := range x.(multiHandle).elements {
		if h != nil {
			if m.balancer[i].Remove(h) {
				removed = true
			}
		}
	}
	return removed
}
