package balancer

import (
	"context"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/stub"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/endpoint"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/info"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/runtime/stats/state"
)

var (
	testData = [...]struct {
		name   string
		add    []endpoint.Endpoint
		del    []endpoint.Endpoint
		banned map[string]struct{}
		repeat int
		exp    map[string]int
		err    bool
	}{
		{
			add: []endpoint.Endpoint{
				{Addr: endpoint.Addr{Host: "foo"}},
				{Addr: endpoint.Addr{Host: "bar"}},
			},
			repeat: 1000,
			exp: map[string]int{
				"foo": 500,
				"bar": 500,
			},
		},
		{
			add: []endpoint.Endpoint{
				{Addr: endpoint.Addr{Host: "foo"}, LoadFactor: 0.2},
				{Addr: endpoint.Addr{Host: "bar"}, LoadFactor: 1},
				{Addr: endpoint.Addr{Host: "baz"}, LoadFactor: 1},
			},
			repeat: 1000,
			exp: map[string]int{
				"foo": 600,
				"bar": 200,
				"baz": 200,
			},
		},
		{
			add: []endpoint.Endpoint{
				{Addr: endpoint.Addr{Host: "foo"}, LoadFactor: 1},
				{Addr: endpoint.Addr{Host: "bar"}, LoadFactor: 0.1},
				{Addr: endpoint.Addr{Host: "baz"}, LoadFactor: 0.9},
			},
			repeat: 1000,
			exp: map[string]int{
				"foo": 200,
				"bar": 600,
				"baz": 200,
			},
		},
		{
			add: []endpoint.Endpoint{
				{Addr: endpoint.Addr{Host: "foo"}, LoadFactor: 0.25},
				{Addr: endpoint.Addr{Host: "bar"}, LoadFactor: 1},
				{Addr: endpoint.Addr{Host: "baz"}, LoadFactor: 1},
			},
			del: []endpoint.Endpoint{
				{Addr: endpoint.Addr{Host: "foo"}},
			},
			repeat: 1000,
			exp: map[string]int{
				"bar": 500,
				"baz": 500,
			},
		},
		{
			add: []endpoint.Endpoint{
				{Addr: endpoint.Addr{Host: "foo"}, LoadFactor: 1},
				{Addr: endpoint.Addr{Host: "bar"}, LoadFactor: 0.25},
				{Addr: endpoint.Addr{Host: "baz"}, LoadFactor: 0.25},
			},
			del: []endpoint.Endpoint{
				{Addr: endpoint.Addr{Host: "foo"}},
			},
			repeat: 1000,
			exp: map[string]int{
				"bar": 500,
				"baz": 500,
			},
		},
		{
			add: []endpoint.Endpoint{
				{Addr: endpoint.Addr{Host: "foo"}, LoadFactor: 1},
				{Addr: endpoint.Addr{Host: "bar"}, LoadFactor: 0.75},
				{Addr: endpoint.Addr{Host: "baz"}, LoadFactor: 0.25},
			},
			del: []endpoint.Endpoint{
				{Addr: endpoint.Addr{Host: "bar"}},
			},
			repeat: 1200,
			exp: map[string]int{
				"foo": 400,
				"baz": 800,
			},
		},
		{
			add: []endpoint.Endpoint{
				{Addr: endpoint.Addr{Host: "foo"}, LoadFactor: 0},
				{Addr: endpoint.Addr{Host: "bar"}, LoadFactor: 0},
				{Addr: endpoint.Addr{Host: "baz"}, LoadFactor: 0},
			},
			del: []endpoint.Endpoint{
				{Addr: endpoint.Addr{Host: "baz"}},
			},
			repeat: 1000,
			exp: map[string]int{
				"foo": 500,
				"bar": 500,
			},
		},
		{
			add: []endpoint.Endpoint{
				{Addr: endpoint.Addr{Host: "foo"}, LoadFactor: 0},
				{Addr: endpoint.Addr{Host: "bar"}, LoadFactor: 0},
				{Addr: endpoint.Addr{Host: "baz"}, LoadFactor: 0},
			},
			del: []endpoint.Endpoint{
				{Addr: endpoint.Addr{Host: "foo"}},
				{Addr: endpoint.Addr{Host: "bar"}},
				{Addr: endpoint.Addr{Host: "baz"}},
			},
			repeat: 1,
			err:    true,
		},
		{
			add: []endpoint.Endpoint{
				{Addr: endpoint.Addr{Host: "foo"}, LoadFactor: 0},
				{Addr: endpoint.Addr{Host: "bar"}, LoadFactor: 0},
				{Addr: endpoint.Addr{Host: "baz"}, LoadFactor: 0},
			},
			banned: map[string]struct{}{
				"foo": {},
				"bar": {},
			},
			repeat: 100,
			err:    true,
			exp: map[string]int{
				"baz": 100,
			},
		},
		{
			add: []endpoint.Endpoint{
				{Addr: endpoint.Addr{Host: "foo"}, LoadFactor: 0},
				{Addr: endpoint.Addr{Host: "bar"}, LoadFactor: 0},
				{Addr: endpoint.Addr{Host: "baz"}, LoadFactor: 0},
			},
			banned: map[string]struct{}{
				"foo": {},
			},
			repeat: 100,
			err:    true,
			exp: map[string]int{
				"bar": 50,
				"baz": 50,
			},
		},
		{
			add: []endpoint.Endpoint{
				{Addr: endpoint.Addr{Host: "foo"}, LoadFactor: 0},
				{Addr: endpoint.Addr{Host: "bar"}, LoadFactor: 0},
				{Addr: endpoint.Addr{Host: "baz"}, LoadFactor: 0},
			},
			banned: map[string]struct{}{
				"foo": {},
				"bar": {},
				"baz": {},
			},
			repeat: 150,
			err:    true,
			exp: map[string]int{
				"foo": 50,
				"bar": 50,
				"baz": 50,
			},
		},
		{
			add: []endpoint.Endpoint{
				{Addr: endpoint.Addr{Host: "foo"}, LoadFactor: 10},
				{Addr: endpoint.Addr{Host: "bar"}, LoadFactor: 20},
				{Addr: endpoint.Addr{Host: "baz"}, LoadFactor: 30},
			},
			banned: map[string]struct{}{
				"foo": {},
				"bar": {},
				"baz": {},
			},
			repeat: 150,
			err:    true,
			exp: map[string]int{
				"foo": 75,
				"bar": 50,
				"baz": 25,
			},
		},
		{
			add: []endpoint.Endpoint{
				{Addr: endpoint.Addr{Host: "foo"}, LoadFactor: 10},
				{Addr: endpoint.Addr{Host: "bar"}, LoadFactor: 20},
				{Addr: endpoint.Addr{Host: "baz"}, LoadFactor: 30},
			},
			banned: map[string]struct{}{
				"foo": {},
			},
			repeat: 150,
			err:    true,
			exp: map[string]int{
				"foo": 0,
				"bar": 100,
				"baz": 50,
			},
		},
	}
)

func TestRoundRobinBalancer(t *testing.T) {
	for _, test := range testData {
		t.Run(test.name, func(t *testing.T) {
			var (
				mconn = map[conn.Conn]string{} // Conn to addr mapping for easy matching.
				maddr = map[string]conn.Conn{} // addr to Conn mapping.
				melem = map[string]Element{}
				mdist = map[string]int{}
			)
			r := new(roundRobin)
			for _, e := range test.add {
				c := conn.New(context.Background(), endpoint.Endpoint{}, nil, stub.Config(config.New()))
				c.Runtime().SetState(state.Online)
				mconn[c] = e.Host
				maddr[e.Host] = c
				melem[e.Host] = r.Insert(c, info.Info{
					LoadFactor: e.LoadFactor,
				})
			}
			for _, e := range test.del {
				r.Remove(melem[e.Host])
			}
			for addr := range test.banned {
				if err := r.Pessimize(melem[addr]); err != nil {
					t.Errorf("unexpected pessimization error: %w", err)
				}
			}
			for i := 0; i < test.repeat; i++ {
				conn := r.Next()
				if conn == nil {
					if len(test.add) > len(test.del) {
						t.Fatal("unexpected no-Conn")
					}
				} else {
					mdist[mconn[conn]]++
				}
			}
			for addr, exp := range test.exp {
				if act := mdist[addr]; act != exp {
					t.Errorf(
						"unexpected distribution for addr %q: %v; want %v",
						addr, act, exp,
					)
				}
				delete(mdist, addr)
			}
			for addr := range mdist {
				t.Fatalf("unexpected addr in distribution: %q", addr)
			}
		})
	}
}

func TestRandomChoiceBalancer(t *testing.T) {
	for _, test := range testData {
		t.Run(test.name, func(t *testing.T) {
			var (
				mconn = map[conn.Conn]string{} // Conn to addr mapping for easy matching.
				maddr = map[string]conn.Conn{} // addr to Conn mapping.
				melem = map[string]Element{}
				mdist = map[string]int{}
			)
			r := new(roundRobin)
			for _, e := range test.add {
				c := conn.New(context.Background(), endpoint.Endpoint{}, nil, stub.Config(config.New()))
				c.Runtime().SetState(state.Online)
				mconn[c] = e.Host
				maddr[e.Host] = c
				melem[e.Host] = r.Insert(c, info.Info{
					LoadFactor: e.LoadFactor,
				})
			}
			for _, e := range test.del {
				r.Remove(melem[e.Host])
			}
			for addr := range test.banned {
				if err := r.Pessimize(melem[addr]); err != nil {
					t.Errorf("unexpected pessimization error: %w", err)
				}
			}
			for i := 0; i < test.repeat; i++ {
				conn := r.Next()
				if conn == nil {
					if len(test.add) > len(test.del) {
						t.Fatal("unexpected no-Conn")
					}
				} else {
					mdist[mconn[conn]]++
				}
			}
			for addr, exp := range test.exp {
				if act := mdist[addr]; act < int(float64(exp)*0.9) || act > int(float64(exp)*1.1) {
					t.Errorf(
						"unexpected distribution for addr %q: %v; want %v",
						addr, act, exp,
					)
				}
				delete(mdist, addr)
			}
			for addr := range mdist {
				t.Fatalf("unexpected addr in distribution: %q", addr)
			}
		})
	}
}
