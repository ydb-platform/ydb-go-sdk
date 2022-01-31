package test

import (
	"context"
	"net"
	"strconv"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/list"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/multi"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/rr"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/stub"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint/info"
)

func isEvenConn(c conn.Conn) bool {
	host, _, err := net.SplitHostPort(c.Endpoint().Address())
	if err != nil {
		panic(err)
	}
	n, err := strconv.Atoi(host)
	if err != nil {
		panic(err)
	}
	return n%2 == 0
}

func isOddConn(c conn.Conn) bool {
	return !isEvenConn(c)
}

func TestMulti(t *testing.T) {
	cs1, b1 := stub.Balancer()
	cs2, b2 := stub.Balancer()
	forEachList := func(it func(*list.List)) {
		it(cs1)
		it(cs2)
	}
	forEachConn := func(it func(conn.Conn, info.Info)) {
		forEachList(func(cs *list.List) {
			for _, e := range *cs {
				it(e.Conn, e.Info)
			}
		})
	}
	m := multi.Balancer(
		multi.WithBalancer(b1, isOddConn),
		multi.WithBalancer(b2, isEvenConn),
	)
	const n = 100
	var (
		es = make([]balancer.Element, n)
		el = make(map[conn.Conn]balancer.Element, n)
	)
	for i := 0; i < n; i++ {
		c := conn.New(endpoint.New(strconv.Itoa(i)+":0"), config.New())
		e := m.Insert(c)
		es[i] = e
		el[c] = e
	}
	forEachList(func(cs *list.List) {
		if act, exp := len(*cs), n/2; act != exp {
			t.Errorf(
				"unexepcted number of conns: %d; want %d",
				act, exp,
			)
		}
	})
	for i := 0; i < n; i++ {
		m.Update(es[i], info.Info{
			LoadFactor: 1,
		})
	}
	forEachConn(func(conn conn.Conn, info info.Info) {
		if act, exp := info.LoadFactor, float32(1); act != exp {
			t.Errorf(
				"unexpected load factor: %f; want %f",
				act, exp,
			)
		}
	})

	// Check first balancer first.
	// Thus, we expect here that until first balancer is not empty
	// balancer will return connections only from it.
	for i := 0; i < n; i++ {
		c := m.Next()
		if !isOddConn(c) {
			t.Fatalf("Next() returned unexpected Conn")
		}
	}
	// Now remove all connections from first iface.
	for i := 0; i < n/2; i++ {
		c := m.Next()
		if isOddConn(c) {
			m.Remove(el[c])
		}
	}
	// And check that balancer returns connections from the second
	// iface.
	for i := 0; i < n; i++ {
		c := m.Next()
		if !isEvenConn(c) {
			t.Fatalf("Next() returned unexpected Conn")
		}
	}
	// Now remove all connections from second iface.
	for i := 0; i < n/2; i++ {
		c := m.Next()
		if isEvenConn(c) {
			m.Remove(el[c])
		}
	}
	if c := m.Next(); c != nil {
		t.Fatalf("Next() returned unexpected non-nil Conn")
	}
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
		c := m.Next()
		if !c.Endpoint().LocalDC() {
			t.Fatalf("Next() returned unexpected Conn")
		}
	}
	// Now remove all connections from first iface.
	for i := 0; i < n/2; i++ {
		c := m.Next()
		if c.Endpoint().LocalDC() {
			m.Remove(el[c])
		}
	}
	// And check that balancer returns connections from the second iface.
	for i := 0; i < n; i++ {
		c := m.Next()
		if c.Endpoint().LocalDC() {
			t.Fatalf("Next() returned unexpected Conn")
		}
	}
	// Now remove all connections from second iface.
	for i := 0; i < n/2; i++ {
		c := m.Next()
		if !c.Endpoint().LocalDC() {
			m.Remove(el[c])
		}
	}
	if c := m.Next(); c != nil {
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
		c := m.Next()
		if c.Endpoint().Address() != preferredEndpoint {
			t.Fatalf("Next() returned unexpected Conn")
		}
	}
	// Now remove all connections from first iface.
	for i := 0; i < 1; i++ {
		c := m.Next()
		if c.Endpoint().Address() == preferredEndpoint {
			m.Remove(el[c])
		}
	}
	// And check that balancer returns connections from the second iface.
	for i := 0; i < n; i++ {
		c := m.Next()
		if c.Endpoint().Address() == preferredEndpoint {
			t.Fatalf("Next() returned unexpected Conn")
		}
	}
	// Now remove all connections from second iface.
	for i := 0; i < n-1; i++ {
		c := m.Next()
		if c.Endpoint().Address() != preferredEndpoint {
			m.Remove(el[c])
		}
	}
	if c := m.Next(); c != nil {
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

func TestRoundRobin(t *testing.T) {
	for _, test := range testData {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
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
				c.SetState(ctx, conn.Online)
				if test.banned != nil {
					if _, ok := test.banned[e.Address()]; ok {
						c.SetState(ctx, conn.Banned)
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

func TestRandomChoice(t *testing.T) {
	multiplier := 100
	for _, test := range testData {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
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
				c.SetState(ctx, conn.Online)
				if _, ok := test.banned[e.Address()]; ok {
					c.SetState(ctx, conn.Banned)
				}
				mconn[c] = e.Address()
				maddr[e.Address()] = c
				melem[e.Address()] = r.Insert(c)
			}
			for _, e := range test.del {
				r.Remove(melem[e.Address()])
			}
			for i := 0; i < test.repeat*multiplier; i++ {
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
