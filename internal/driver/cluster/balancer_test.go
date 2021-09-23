package cluster

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer"
	"strconv"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/info"
)

func isEvenConn(c conn.Conn, _ info.Info) bool {
	n, err := strconv.Atoi(c.Addr().String())
	if err != nil {
		panic(err)
	}
	return n%2 == 0
}

func isOddConn(c conn.Conn, info info.Info) bool {
	return !isEvenConn(c, info)
}

func TestMultiBalancer(t *testing.T) {
	cs1, b1 := simpleBalancer()
	cs2, b2 := simpleBalancer()
	forEachList := func(it func(*connList)) {
		it(cs1)
		it(cs2)
	}
	forEachConn := func(it func(conn.Conn, info.Info)) {
		forEachList(func(cs *connList) {
			for _, e := range *cs {
				it(e.conn, e.info)
			}
		})
	}
	m := balancer.NewMultiBalancer(
		balancer.WithBalancer(b1, isOddConn),
		balancer.WithBalancer(b2, isEvenConn),
	)
	const n = 100
	var (
		es = make([]balancer.Element, n)
		el = make(map[conn.Conn]balancer.Element, n)
	)
	for i := 0; i < n; i++ {
		c := &conn.conn{
			addr: conn.connAddr{
				addr: strconv.Itoa(i),
			},
		}
		e := m.Insert(c, info.Info{})
		es[i] = e
		el[c] = e
	}
	forEachList(func(cs *connList) {
		if act, exp := len(*cs), n/2; act != exp {
			t.Errorf(
				"unexepcted number of conns: %d; want %d",
				act, exp,
			)
		}
	})
	for i := 0; i < n; i++ {
		m.Update(es[i], info.Info{
			loadFactor: 1,
		})
	}
	forEachConn(func(conn conn.Conn, info info.Info) {
		if act, exp := info.loadFactor, float32(1); act != exp {
			t.Errorf(
				"unexpected load factor: %f; want %f",
				act, exp,
			)
		}
	})

	// Multibalancer must check first Balancer first.
	// Thus, we expect here that until first Balancer is not empty
	// multibalancer will return connections only from it.
	for i := 0; i < n; i++ {
		c := m.Next()
		if !isOddConn(c, info.Info{}) {
			t.Fatalf("Next() returned unexpected Conn")
		}
	}
	// Now remove all connections from first Balancer.
	for i := 0; i < n/2; i++ {
		c := m.Next()
		if isOddConn(c, info.Info{}) {
			m.Remove(el[c])
		}
	}
	// And check that multibalancer returns connections from the second
	// Balancer.
	for i := 0; i < n; i++ {
		c := m.Next()
		if !isEvenConn(c, info.Info{}) {
			t.Fatalf("Next() returned unexpected Conn")
		}
	}
	// Now remove all connections from second Balancer.
	for i := 0; i < n/2; i++ {
		c := m.Next()
		if isEvenConn(c, info.Info{}) {
			m.Remove(el[c])
		}
	}
	if c := m.Next(); c != nil {
		t.Fatalf("Next() returned unexpected non-nil Conn")
	}
}
