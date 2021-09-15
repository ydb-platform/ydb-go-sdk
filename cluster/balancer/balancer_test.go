package balancer

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/conn"
	"strconv"
	"testing"
)

func isEvenConn(c *conn.conn, _ cluster.connInfo) bool {
	n, err := strconv.Atoi(c.addr.addr)
	if err != nil {
		panic(err)
	}
	return n%2 == 0
}

func isOddConn(c *conn.conn, info cluster.connInfo) bool {
	return !isEvenConn(c, info)
}

func TestMultiBalancer(t *testing.T) {
	cs1, b1 := cluster.simpleBalancer()
	cs2, b2 := cluster.simpleBalancer()
	forEachList := func(it func(*cluster.connList)) {
		it(cs1)
		it(cs2)
	}
	forEachConn := func(it func(*conn.conn, cluster.connInfo)) {
		forEachList(func(cs *cluster.connList) {
			for _, e := range *cs {
				it(e.conn, e.info)
			}
		})
	}
	m := newMultiBalancer(
		withBalancer(b1, isOddConn),
		withBalancer(b2, isEvenConn),
	)
	const n = 100
	var (
		es = make([]BalancerElement, n)
		el = make(map[*conn.conn]BalancerElement, n)
	)
	for i := 0; i < n; i++ {
		c := &conn.conn{
			addr: conn.connAddr{
				addr: strconv.Itoa(i),
			},
		}
		e := m.Insert(c, cluster.connInfo{})
		es[i] = e
		el[c] = e
	}
	forEachList(func(cs *cluster.connList) {
		if act, exp := len(*cs), n/2; act != exp {
			t.Errorf(
				"unexepcted number of conns: %d; want %d",
				act, exp,
			)
		}
	})
	for i := 0; i < n; i++ {
		m.Update(es[i], cluster.connInfo{
			loadFactor: 1,
		})
	}
	forEachConn(func(conn *conn.conn, info cluster.connInfo) {
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
		if !isOddConn(c, cluster.connInfo{}) {
			t.Fatalf("Next() returned unexpected Conn")
		}
	}
	// Now remove all connections from first Balancer.
	for i := 0; i < n/2; i++ {
		c := m.Next()
		if isOddConn(c, cluster.connInfo{}) {
			m.Remove(el[c])
		}
	}
	// And check that multibalancer returns connections from the second
	// Balancer.
	for i := 0; i < n; i++ {
		c := m.Next()
		if !isEvenConn(c, cluster.connInfo{}) {
			t.Fatalf("Next() returned unexpected Conn")
		}
	}
	// Now remove all connections from second Balancer.
	for i := 0; i < n/2; i++ {
		c := m.Next()
		if isEvenConn(c, cluster.connInfo{}) {
			m.Remove(el[c])
		}
	}
	if c := m.Next(); c != nil {
		t.Fatalf("Next() returned unexpected non-nil Conn")
	}
}
