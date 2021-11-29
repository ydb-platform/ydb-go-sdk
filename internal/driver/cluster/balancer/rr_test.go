package balancer

import (
	"context"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/info"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/stub"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/state"
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
				endpoint.New("foo:0"),
				endpoint.New("bar:0"),
			},
			repeat: 1000,
			exp: map[string]int{
				"foo:0": 500,
				"bar:0": 500,
			},
		},
		{
			add: []endpoint.Endpoint{
				endpoint.New("foo:0", endpoint.WithLoadFactor(0.2)),
				endpoint.New("bar:0", endpoint.WithLoadFactor(1)),
				endpoint.New("baz:0", endpoint.WithLoadFactor(1)),
			},
			repeat: 1000,
			exp: map[string]int{
				"foo:0": 600,
				"bar:0": 200,
				"baz:0": 200,
			},
		},
		{
			add: []endpoint.Endpoint{
				endpoint.New("foo:0", endpoint.WithLoadFactor(1)),
				endpoint.New("bar:0", endpoint.WithLoadFactor(0.1)),
				endpoint.New("baz:0", endpoint.WithLoadFactor(0.9)),
			},
			repeat: 1000,
			exp: map[string]int{
				"foo:0": 200,
				"bar:0": 600,
				"baz:0": 200,
			},
		},
		{
			add: []endpoint.Endpoint{
				endpoint.New("foo:0", endpoint.WithLoadFactor(0.25)),
				endpoint.New("bar:0", endpoint.WithLoadFactor(1)),
				endpoint.New("baz:0", endpoint.WithLoadFactor(1)),
			},
			del: []endpoint.Endpoint{
				endpoint.New("foo:0"),
			},
			repeat: 1000,
			exp: map[string]int{
				"bar:0": 500,
				"baz:0": 500,
			},
		},
		{
			add: []endpoint.Endpoint{
				endpoint.New("foo:0", endpoint.WithLoadFactor(1)),
				endpoint.New("bar:0", endpoint.WithLoadFactor(0.25)),
				endpoint.New("baz:0", endpoint.WithLoadFactor(0.25)),
			},
			del: []endpoint.Endpoint{
				endpoint.New("foo:0"),
			},
			repeat: 1000,
			exp: map[string]int{
				"bar:0": 500,
				"baz:0": 500,
			},
		},
		{
			add: []endpoint.Endpoint{
				endpoint.New("foo:0", endpoint.WithLoadFactor(1)),
				endpoint.New("bar:0", endpoint.WithLoadFactor(0.75)),
				endpoint.New("baz:0", endpoint.WithLoadFactor(0.25)),
			},
			del: []endpoint.Endpoint{
				endpoint.New("bar:0"),
			},
			repeat: 1200,
			exp: map[string]int{
				"foo:0": 400,
				"baz:0": 800,
			},
		},
		{
			add: []endpoint.Endpoint{
				endpoint.New("foo:0", endpoint.WithLoadFactor(0)),
				endpoint.New("bar:0", endpoint.WithLoadFactor(0)),
				endpoint.New("baz:0", endpoint.WithLoadFactor(0)),
			},
			del: []endpoint.Endpoint{
				endpoint.New("baz:0"),
			},
			repeat: 1000,
			exp: map[string]int{
				"foo:0": 500,
				"bar:0": 500,
			},
		},
		{
			add: []endpoint.Endpoint{
				endpoint.New("foo:0", endpoint.WithLoadFactor(0)),
				endpoint.New("bar:0", endpoint.WithLoadFactor(0)),
				endpoint.New("baz:0", endpoint.WithLoadFactor(0)),
			},
			del: []endpoint.Endpoint{
				endpoint.New("foo:0"),
				endpoint.New("bar:0"),
				endpoint.New("baz:0"),
			},
			repeat: 1,
			err:    true,
		},
		{
			add: []endpoint.Endpoint{
				endpoint.New("foo:0", endpoint.WithLoadFactor(0)),
				endpoint.New("bar:0", endpoint.WithLoadFactor(0)),
				endpoint.New("baz:0", endpoint.WithLoadFactor(0)),
			},
			banned: map[string]struct{}{
				"foo:0": {},
				"bar:0": {},
			},
			repeat: 100,
			err:    true,
			exp: map[string]int{
				"baz:0": 100,
			},
		},
		{
			add: []endpoint.Endpoint{
				endpoint.New("foo:0", endpoint.WithLoadFactor(0)),
				endpoint.New("bar:0", endpoint.WithLoadFactor(0)),
				endpoint.New("baz:0", endpoint.WithLoadFactor(0)),
			},
			banned: map[string]struct{}{
				"foo:0": {},
			},
			repeat: 100,
			err:    true,
			exp: map[string]int{
				"bar:0": 50,
				"baz:0": 50,
			},
		},
		{
			add: []endpoint.Endpoint{
				endpoint.New("foo:0", endpoint.WithLoadFactor(0)),
				endpoint.New("bar:0", endpoint.WithLoadFactor(0)),
				endpoint.New("baz:0", endpoint.WithLoadFactor(0)),
			},
			banned: map[string]struct{}{
				"foo:0": {},
				"bar:0": {},
				"baz:0": {},
			},
			repeat: 150,
			err:    true,
			exp: map[string]int{
				"foo:0": 50,
				"bar:0": 50,
				"baz:0": 50,
			},
		},
		{
			add: []endpoint.Endpoint{
				endpoint.New("foo:0", endpoint.WithLoadFactor(10)),
				endpoint.New("bar:0", endpoint.WithLoadFactor(20)),
				endpoint.New("baz:0", endpoint.WithLoadFactor(30)),
			},
			banned: map[string]struct{}{
				"foo:0": {},
				"bar:0": {},
				"baz:0": {},
			},
			repeat: 150,
			err:    true,
			exp: map[string]int{
				"foo:0": 75,
				"bar:0": 50,
				"baz:0": 25,
			},
		},
		{
			add: []endpoint.Endpoint{
				endpoint.New("foo:0", endpoint.WithLoadFactor(10)),
				endpoint.New("bar:0", endpoint.WithLoadFactor(20)),
				endpoint.New("baz:0", endpoint.WithLoadFactor(30)),
			},
			banned: map[string]struct{}{
				"foo:0": {},
			},
			repeat: 150,
			err:    true,
			exp: map[string]int{
				"foo:0": 0,
				"bar:0": 100,
				"baz:0": 50,
			},
		},
	}
)

func TestRoundRobinBalancer(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping testing in non-short mode")
	}

	for _, test := range testData {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var (
				mconn = map[conn.Conn]string{} // Conn to addr mapping for easy matching.
				maddr = map[string]conn.Conn{} // addr to Conn mapping.
				melem = map[string]Element{}
				mdist = map[string]int{}
			)
			r := new(roundRobin)
			for _, e := range test.add {
				c := conn.New(
					e,
					nil,
					stub.Config(
						config.New(
							config.WithDatabase("test"),
							config.WithEndpoint("test"),
						),
					),
				)
				c.SetState(ctx, state.Online)
				if test.banned != nil {
					if _, ok := test.banned[e.Address()]; ok {
						c.SetState(ctx, state.Banned)
					}
				}
				mconn[c] = e.Address()
				maddr[e.Address()] = c
				melem[e.Address()] = r.Insert(c, info.Info{
					LoadFactor: e.LoadFactor(),
				})
			}
			for _, e := range test.del {
				r.Remove(melem[e.Address()])
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
	if !testing.Short() {
		t.Skip("skipping testing in non-short mode")
	}

	for _, test := range testData {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var (
				mconn = map[conn.Conn]string{} // Conn to addr mapping for easy matching.
				maddr = map[string]conn.Conn{} // addr to Conn mapping.
				melem = map[string]Element{}
				mdist = map[string]int{}
			)
			r := new(roundRobin)
			for _, e := range test.add {
				c := conn.New(
					e,
					nil,
					stub.Config(config.New()),
				)
				c.SetState(ctx, state.Online)
				if _, ok := test.banned[e.Address()]; ok {
					c.SetState(ctx, state.Banned)
				}
				mconn[c] = e.Address()
				maddr[e.Address()] = c
				melem[e.Address()] = r.Insert(c, info.Info{
					LoadFactor: e.LoadFactor(),
				})
			}
			for _, e := range test.del {
				r.Remove(melem[e.Address()])
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
