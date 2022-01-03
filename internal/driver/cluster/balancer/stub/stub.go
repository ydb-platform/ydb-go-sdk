package stub

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/info"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/list"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/iface"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/state"
)

type stubBalancer struct {
	OnNext      func() conn.Conn
	OnInsert    func(conn.Conn, info.Info) iface.Element
	OnUpdate    func(iface.Element, info.Info)
	OnRemove    func(iface.Element)
	OnPessimize func(context.Context, iface.Element) error
	OnContains  func(iface.Element) bool
}

func Stub() (*list.List, iface.Balancer) {
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
		OnInsert: func(conn conn.Conn, info info.Info) iface.Element {
			return cs.Insert(conn, info)
		},
		OnRemove: func(x iface.Element) {
			e := x.(*list.Element)
			cs.Remove(e)
		},
		OnUpdate: func(x iface.Element, info info.Info) {
			e := x.(*list.Element)
			e.Info = info
		},
		OnPessimize: func(ctx context.Context, x iface.Element) error {
			e := x.(*list.Element)
			e.Conn.SetState(ctx, state.Banned)
			return nil
		},
		OnContains: func(x iface.Element) bool {
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

func (s stubBalancer) Insert(c conn.Conn, i info.Info) iface.Element {
	if f := s.OnInsert; f != nil {
		return f(c, i)
	}
	return nil
}

func (s stubBalancer) Update(el iface.Element, i info.Info) {
	if f := s.OnUpdate; f != nil {
		f(el, i)
	}
}

func (s stubBalancer) Remove(el iface.Element) {
	if f := s.OnRemove; f != nil {
		f(el)
	}
}

func (s stubBalancer) Pessimize(ctx context.Context, el iface.Element) error {
	if f := s.OnPessimize; f != nil {
		return f(ctx, el)
	}
	return nil
}

func (s stubBalancer) Contains(el iface.Element) bool {
	if f := s.OnContains; f != nil {
		return f(el)
	}
	return false
}
