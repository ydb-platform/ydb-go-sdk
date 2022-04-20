package test

import (
	"strconv"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/multi"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/rr"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
)

func TestMulti(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {

	})
}

func TestPreferLocal(t *testing.T) {
	m := multi.Balancer(
		multi.WithBalancer(
			rr.RoundRobin(),
			func(cc conn.Conn) bool {
				return cc.Endpoint().LocalDC()
			},
		),
		multi.WithBalancer(
			rr.RoundRobin(),
			func(cc conn.Conn) bool {
				return !cc.Endpoint().LocalDC()
			},
		),
	)
	const n = 100
	var (
		es = make([]balancer.Element, n)
		el = make(map[conn.Conn]balancer.Element, n)
	)
	for i := 0; i < n; i++ {
		c := conn.New(
			endpoint.New(
				strconv.Itoa(i)+":0",
				endpoint.WithLocalDC(i%2 == 0),
			),
			config.New(),
		)
		e := m.Insert(c)
		es[i] = e
		el[c] = e
	}

	// Check first balancer first.
	// Thus, we expect here that until first balancer is not empty
	// balancer will return connections only from it.
	for i := 0; i < n; i++ {
		c := m.Next(nil, false)
		if !c.Endpoint().LocalDC() {
			t.Fatalf("Next() returned unexpected Conn")
		}
	}
	// Now remove all connections from first iface.
	for i := 0; i < n/2; i++ {
		c := m.Next(nil, false)
		if c.Endpoint().LocalDC() {
			m.Remove(el[c])
		}
	}
	// And check that balancer returns connections from the second iface.
	for i := 0; i < n; i++ {
		c := m.Next(nil, false)
		if c.Endpoint().LocalDC() {
			t.Fatalf("Next() returned unexpected Conn")
		}
	}
	// Now remove all connections from second iface.
	for i := 0; i < n/2; i++ {
		c := m.Next(nil, false)
		if !c.Endpoint().LocalDC() {
			m.Remove(el[c])
		}
	}
	if c := m.Next(nil, false); c != nil {
		t.Fatalf("Next() returned unexpected non-nil Conn")
	}
}

func TestPreferEndpoint(t *testing.T) {
	preferredEndpoint := "23:0"
	m := multi.Balancer(
		multi.WithBalancer(
			rr.RoundRobin(),
			func(cc conn.Conn) bool {
				return cc.Endpoint().Address() == preferredEndpoint
			},
		),
		multi.WithBalancer(
			rr.RoundRobin(),
			func(cc conn.Conn) bool {
				return cc.Endpoint().Address() != preferredEndpoint
			},
		),
	)
	const n = 100
	var (
		es = make([]balancer.Element, n)
		el = make(map[conn.Conn]balancer.Element, n)
	)
	for i := 0; i < n; i++ {
		c := conn.New(
			endpoint.New(
				strconv.Itoa(i)+":0",
				endpoint.WithLocalDC(i%2 == 0),
			),
			config.New(),
		)
		e := m.Insert(c)
		es[i] = e
		el[c] = e
	}

	// Check first balancer first.
	// Thus, we expect here that until first balancer is not empty
	// balancer will return connections only from it.
	for i := 0; i < n; i++ {
		c := m.Next(nil, false)
		if c.Endpoint().Address() != preferredEndpoint {
			t.Fatalf("Next() returned unexpected Conn")
		}
	}
	// Now remove all connections from first iface.
	for i := 0; i < 1; i++ {
		c := m.Next(nil, false)
		if c.Endpoint().Address() == preferredEndpoint {
			m.Remove(el[c])
		}
	}
	// And check that balancer returns connections from the second iface.
	for i := 0; i < n; i++ {
		c := m.Next(nil, false)
		if c.Endpoint().Address() == preferredEndpoint {
			t.Fatalf("Next() returned unexpected Conn")
		}
	}
	// Now remove all connections from second iface.
	for i := 0; i < n-1; i++ {
		c := m.Next(nil, false)
		if c.Endpoint().Address() != preferredEndpoint {
			m.Remove(el[c])
		}
	}
	if c := m.Next(nil, false); c != nil {
		t.Fatalf("Next() returned unexpected non-nil Conn")
	}
}

var testData = [...]struct {
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
			endpoint.New("foo:0"),
			endpoint.New("bar:0"),
			endpoint.New("baz:0"),
		},
		repeat: 999,
		exp: map[string]int{
			"foo:0": 333,
			"bar:0": 333,
			"baz:0": 333,
		},
	},
	{
		add: []endpoint.Endpoint{
			endpoint.New("foo:0"),
			endpoint.New("bar:0"),
			endpoint.New("baz:0"),
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
			endpoint.New("foo:0"),
			endpoint.New("bar:0"),
			endpoint.New("baz:0"),
		},
		del: []endpoint.Endpoint{
			endpoint.New("bar:0"),
		},
		repeat: 1200,
		exp: map[string]int{
			"foo:0": 600,
			"baz:0": 600,
		},
	},
	{
		add: []endpoint.Endpoint{
			endpoint.New("foo:0"),
			endpoint.New("bar:0"),
			endpoint.New("baz:0"),
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
			endpoint.New("foo:0"),
			endpoint.New("bar:0"),
			endpoint.New("baz:0"),
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
			endpoint.New("foo:0"),
			endpoint.New("bar:0"),
			endpoint.New("baz:0"),
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
			endpoint.New("foo:0"),
			endpoint.New("bar:0"),
			endpoint.New("baz:0"),
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
			endpoint.New("foo:0"),
			endpoint.New("bar:0"),
			endpoint.New("baz:0"),
		},
		banned: map[string]struct{}{
			"foo:0": {},
		},
		repeat: 150,
		err:    true,
		exp: map[string]int{
			"foo:0": 0,
			"bar:0": 75,
			"baz:0": 75,
		},
	},
}

func TestRoundRobin(t *testing.T) {
	for _, test := range testData {
		t.Run(test.name, func(t *testing.T) {
			var (
				mconn = map[conn.Conn]string{} // Conn to addr mapping for easy matching.
				maddr = map[string]conn.Conn{} // addr to Conn mapping.
				melem = map[string]balancer.Element{}
				mdist = map[string]int{}
			)
			r := rr.RoundRobin()
			for _, e := range test.add {
				c := conn.New(
					e,
					config.New(
						config.WithDatabase("test"),
						config.WithEndpoint("test"),
					),
				)
				c.SetState(conn.Online)
				if test.banned != nil {
					if _, ok := test.banned[e.Address()]; ok {
						c.SetState(conn.Banned)
					}
				}
				mconn[c] = e.Address()
				maddr[e.Address()] = c
				melem[e.Address()] = r.Insert(c)
			}
			for _, e := range test.del {
				r.Remove(melem[e.Address()])
			}
			for i := 0; i < test.repeat; i++ {
				conn := r.Next(nil, false)
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

func TestRandomChoice(t *testing.T) {
	multiplier := 100
	for _, test := range testData {
		t.Run(test.name, func(t *testing.T) {
			var (
				mconn = map[conn.Conn]string{} // Conn to addr mapping for easy matching.
				maddr = map[string]conn.Conn{} // addr to Conn mapping.
				melem = map[string]balancer.Element{}
				mdist = map[string]int{}
			)
			r := rr.RandomChoice()
			for _, e := range test.add {
				c := conn.New(
					e,
					config.New(),
				)
				c.SetState(conn.Online)
				if _, ok := test.banned[e.Address()]; ok {
					c.SetState(conn.Banned)
				}
				mconn[c] = e.Address()
				maddr[e.Address()] = c
				melem[e.Address()] = r.Insert(c)
			}
			for _, e := range test.del {
				r.Remove(melem[e.Address()])
			}
			for i := 0; i < test.repeat*multiplier; i++ {
				conn := r.Next(nil, false)
				if conn == nil {
					if len(test.add) > len(test.del) {
						t.Fatal("unexpected no-Conn")
					}
				} else {
					mdist[mconn[conn]]++
				}
			}
			for addr, exp := range test.exp {
				exp *= multiplier
				if act := mdist[addr]; act < int(float64(exp)*0.9) || act > int(float64(exp)*1.1) {
					t.Errorf(
						"unexpected distribution for addr %q: %v; want between %v and %v",
						addr, act, int(float64(exp)*0.9), int(float64(exp)*1.1),
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
