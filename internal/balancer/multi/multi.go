package multi

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/ibalancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint/info"
)

func Balancer(opts ...Option) ibalancer.CreatorBalancer {
	m := new(balancer)
	for _, opt := range opts {
		opt(m)
	}
	return m
}

type multiHandle struct {
	elements []ibalancer.Element
}

type balancer struct {
	balancer []ibalancer.Balancer
	filter   []func(conn.Conn) bool
}

func (b *balancer) Create() ibalancer.Balancer {
	bb := b.balancer
	for i := range bb {
		bb[i] = bb[i].(ibalancer.CreatorBalancer).Create()
	}
	return &balancer{
		balancer: bb,
		filter:   b.filter,
	}
}

func WithBalancer(b ibalancer.Balancer, filter func(cc conn.Conn) bool) Option {
	return func(m *balancer) {
		m.balancer = append(m.balancer, b)
		m.filter = append(m.filter, filter)
	}
}

type Option func(*balancer)

func (b *balancer) Contains(x ibalancer.Element) bool {
	for i, x := range x.(multiHandle).elements {
		if x == nil {
			continue
		}
		if b.balancer[i].Contains(x) {
			return true
		}
	}
	return false
}

func (b *balancer) Next() conn.Conn {
	for _, b := range b.balancer {
		if c := b.Next(); c != nil {
			return c
		}
	}
	return nil
}

func (b *balancer) Insert(conn conn.Conn) ibalancer.Element {
	n := len(b.filter)
	h := multiHandle{
		elements: make([]ibalancer.Element, n),
	}
	for i, f := range b.filter {
		if f(conn) {
			x := b.balancer[i].Insert(conn)
			h.elements[i] = x
		}
	}
	return h
}

func (b *balancer) Update(x ibalancer.Element, info info.Info) {
	for i, x := range x.(multiHandle).elements {
		if x != nil {
			b.balancer[i].Update(x, info)
		}
	}
}

func (b *balancer) Remove(x ibalancer.Element) {
	for i, x := range x.(multiHandle).elements {
		if x != nil {
			b.balancer[i].Remove(x)
		}
	}
}
