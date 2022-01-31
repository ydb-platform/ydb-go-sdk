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

func (b *multi) Create() balancer.Balancer {
	bb := b.balancer
	for i := range bb {
		bb[i] = bb[i].(balancer.Creator).Create()
	}
	return &multi{
		balancer: bb,
		filter:   b.filter,
	}
}

func WithBalancer(b balancer.Balancer, filter func(cc conn.Conn) bool) Option {
	return func(m *multi) {
		m.balancer = append(m.balancer, b)
		m.filter = append(m.filter, filter)
	}
}

type Option func(*multi)

func (b *multi) Contains(x balancer.Element) bool {
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

func (b *multi) Next() conn.Conn {
	for _, b := range b.balancer {
		if c := b.Next(); c != nil {
			return c
		}
	}
	return nil
}

func (b *multi) Insert(conn conn.Conn) balancer.Element {
	n := len(b.filter)
	h := multiHandle{
		elements: make([]balancer.Element, n),
	}
	for i, f := range b.filter {
		if f(conn) {
			x := b.balancer[i].Insert(conn)
			h.elements[i] = x
		}
	}
	return h
}

func (b *multi) Update(x balancer.Element, info info.Info) {
	for i, x := range x.(multiHandle).elements {
		if x != nil {
			b.balancer[i].Update(x, info)
		}
	}
}

func (b *multi) Remove(x balancer.Element) {
	for i, x := range x.(multiHandle).elements {
		if x != nil {
			b.balancer[i].Remove(x)
		}
	}
}
