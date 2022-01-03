package multi

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/info"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/iface"
)

func Balancer(opts ...Option) iface.Balancer {
	m := new(multiBalancer)
	for _, opt := range opts {
		opt(m)
	}
	return m
}

type multiHandle struct {
	elements []iface.Element
}

type multiBalancer struct {
	balancer []iface.Balancer
	filter   []func(conn.Conn, info.Info) bool
}

func WithBalancer(b iface.Balancer, filter func(conn.Conn, info.Info) bool) Option {
	return func(m *multiBalancer) {
		m.balancer = append(m.balancer, b)
		m.filter = append(m.filter, filter)
	}
}

type Option func(*multiBalancer)

func (m *multiBalancer) Contains(x iface.Element) bool {
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

func (m *multiBalancer) Insert(conn conn.Conn, info info.Info) iface.Element {
	n := len(m.filter)
	h := multiHandle{
		elements: make([]iface.Element, n),
	}
	for i, f := range m.filter {
		if f(conn, info) {
			x := m.balancer[i].Insert(conn, info)
			h.elements[i] = x
		}
	}
	return h
}

func (m *multiBalancer) Update(x iface.Element, info info.Info) {
	for i, x := range x.(multiHandle).elements {
		if x != nil {
			m.balancer[i].Update(x, info)
		}
	}
}

func (m *multiBalancer) Remove(x iface.Element) {
	for i, x := range x.(multiHandle).elements {
		if x != nil {
			m.balancer[i].Remove(x)
		}
	}
}
