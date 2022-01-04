package multi

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/ibalancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint/info"
)

func Balancer(opts ...Option) ibalancer.Balancer {
	m := new(multiBalancer)
	for _, opt := range opts {
		opt(m)
	}
	return m
}

type multiHandle struct {
	elements []ibalancer.Element
}

type multiBalancer struct {
	balancer []ibalancer.Balancer
	filter   []func(conn.Conn) bool
}

func WithBalancer(b ibalancer.Balancer, filter func(cc conn.Conn) bool) Option {
	return func(m *multiBalancer) {
		m.balancer = append(m.balancer, b)
		m.filter = append(m.filter, filter)
	}
}

type Option func(*multiBalancer)

func (m *multiBalancer) Contains(x ibalancer.Element) bool {
	for i, x := range x.(multiHandle).elements {
		if x == nil {
			continue
		}
		if m.balancer[i].Contains(x) {
			return true
		}
	}
	return false
}

func (m *multiBalancer) Next() conn.Conn {
	for _, b := range m.balancer {
		if c := b.Next(); c != nil {
			return c
		}
	}
	return nil
}

func (m *multiBalancer) Insert(conn conn.Conn) ibalancer.Element {
	n := len(m.filter)
	h := multiHandle{
		elements: make([]ibalancer.Element, n),
	}
	for i, f := range m.filter {
		if f(conn) {
			x := m.balancer[i].Insert(conn)
			h.elements[i] = x
		}
	}
	return h
}

func (m *multiBalancer) Update(x ibalancer.Element, info info.Info) {
	for i, x := range x.(multiHandle).elements {
		if x != nil {
			m.balancer[i].Update(x, info)
		}
	}
}

func (m *multiBalancer) Remove(x ibalancer.Element) {
	for i, x := range x.(multiHandle).elements {
		if x != nil {
			m.balancer[i].Remove(x)
		}
	}
}
