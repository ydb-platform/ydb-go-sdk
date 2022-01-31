package stub

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/list"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint/info"
)

type stubBalancer struct {
	OnNext      func() conn.Conn
	OnInsert    func(conn.Conn) balancer.Element
	OnUpdate    func(balancer.Element, info.Info)
	OnRemove    func(balancer.Element)
	OnPessimize func(context.Context, balancer.Element) error
	OnContains  func(balancer.Element) bool
}

func Balancer() (*list.List, balancer.Balancer) {
	cs := new(list.List)
	var i int
	return cs, stubBalancer{
		OnNext: func() conn.Conn {
			n := len(*cs)
			if n == 0 {
				return nil
			}
			e := (*cs)[i%n]
			i++
			return e.Conn
		},
		OnInsert: func(conn conn.Conn) balancer.Element {
			return cs.Insert(conn)
		},
		OnRemove: func(x balancer.Element) {
			e := x.(*list.Element)
			cs.Remove(e)
		},
		OnUpdate: func(x balancer.Element, info info.Info) {
			e := x.(*list.Element)
			e.Info = info
		},
		OnPessimize: func(ctx context.Context, x balancer.Element) error {
			e := x.(*list.Element)
			e.Conn.SetState(ctx, conn.Banned)
			return nil
		},
		OnContains: func(x balancer.Element) bool {
			e := x.(*list.Element)
			return cs.Contains(e)
		},
	}
}

func (s stubBalancer) Next() conn.Conn {
	if f := s.OnNext; f != nil {
		return f()
	}
	return nil
}

func (s stubBalancer) Insert(c conn.Conn) balancer.Element {
	if f := s.OnInsert; f != nil {
		return f(c)
	}
	return nil
}

func (s stubBalancer) Update(el balancer.Element, i info.Info) {
	if f := s.OnUpdate; f != nil {
		f(el, i)
	}
}

func (s stubBalancer) Remove(el balancer.Element) {
	if f := s.OnRemove; f != nil {
		f(el)
	}
}

func (s stubBalancer) Pessimize(ctx context.Context, el balancer.Element) error {
	if f := s.OnPessimize; f != nil {
		return f(ctx, el)
	}
	return nil
}

func (s stubBalancer) Contains(el balancer.Element) bool {
	if f := s.OnContains; f != nil {
		return f(el)
	}
	return false
}
