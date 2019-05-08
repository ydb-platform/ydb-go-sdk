package ydb

import (
	"context"
	"sort"
	"strings"
	"sync"
)

// balancerElement is an interface that holds some balancer specific data.
type balancerElement interface {
}

// connInfo contains connection "static" stats – e.g. such that obtained from
// discovery routine.
type connInfo struct {
	loadFactor float32
}

// connEntry represents inserted into the cluster connection.
type connEntry struct {
	conn   *conn
	info   connInfo
	handle balancerElement
}

// balancer is an interface that implements particular load-balancing
// algorithm.
//
// balancer methods called synchronized. That is, implementations must not
// provide additional goroutine safety.
type balancer interface {
	// Next returns next connection for request.
	// Next MUST not return nil if it has at least one connection.
	Next() *conn

	// Insert inserts new connection.
	Insert(*conn, connInfo) balancerElement

	// Update updates previously inserted connection.
	Update(balancerElement, connInfo)

	// Remove removes previously inserted connection.
	Remove(balancerElement)
}

type cluster struct {
	dial     func(context.Context, string, int) (*conn, error)
	balancer balancer

	mu     sync.RWMutex
	once   sync.Once
	index  map[connAddr]connEntry
	wait   chan struct{}
	closed bool
}

func (c *cluster) init() {
	c.once.Do(func() {
		c.index = make(map[connAddr]connEntry)
	})
}

func (c *cluster) Close() (err error) {
	var dummy bool
	c.once.Do(func() {
		dummy = true
	})
	if dummy {
		return
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true

	wait := c.wait
	c.wait = nil

	index := c.index
	c.index = nil

	c.mu.Unlock()

	if wait != nil {
		close(wait)
	}
	for _, entry := range index {
		entry.conn.conn.Close()
	}

	return
}

// Get returns next available connection.
// It returns error on given context cancelation or when cluster become closed.
func (c *cluster) Get(ctx context.Context) (conn *conn, err error) {
	for {
		c.mu.RLock()
		closed := c.closed
		wait := c.await()
		size := len(c.index)
		conn = c.balancer.Next()
		c.mu.RUnlock()
		if closed {
			return nil, ErrClosed
		}
		switch {
		case size == 0 && conn != nil:
			panic("ydb: driver: empty balancer has returned non-nil conn")
		case size != 0 && conn == nil:
			panic("ydb: driver: non-empty balancer has returned nil conn")
		case conn != nil:
			return conn, nil
		}
		select {
		case <-wait():
			// Continue.
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Insert inserts new connection into the cluster.
func (c *cluster) Insert(ctx context.Context, e Endpoint) {
	c.init()

	// Dial not under the mutex.
	conn, err := c.dial(ctx, e.Addr, e.Port)
	if err != nil {
		// Current implementation just gives up on first dial error.
		// But further versions may try to reestablish connection in
		// background. Thats why we silently return here.
		//
		// TODO(kamardin): redial in background. After that Insert(), Remove()
		// and Update() may panic while performing operations on non-existing
		// endpoint.
		return
	}

	addr := connAddr{e.Addr, e.Port}
	info := connInfo{
		loadFactor: e.LoadFactor,
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}

	_, has := c.index[addr]
	if has {
		c.mu.Unlock()
		return
	}
	entry := connEntry{
		conn:   conn,
		info:   info,
		handle: c.balancer.Insert(conn, info),
	}
	c.index[addr] = entry

	wait := c.wait
	c.wait = nil
	c.mu.Unlock()

	if wait != nil {
		close(wait)
	}
}

// Update updates existing connection's runtime stats such that load factor and
// others.
func (c *cluster) Update(ctx context.Context, e Endpoint) {
	addr := connAddr{e.Addr, e.Port}
	info := connInfo{
		loadFactor: e.LoadFactor,
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}

	entry, has := c.index[addr]
	if !has {
		return
	}
	entry.info = info
	c.index[addr] = entry

	c.balancer.Update(entry.handle, info)
}

// Remove removes and closes previously inserted connection.
func (c *cluster) Remove(_ context.Context, e Endpoint) {
	addr := connAddr{e.Addr, e.Port}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}

	entry, has := c.index[addr]
	if !has {
		c.mu.Unlock()
		return
	}
	delete(c.index, addr)

	c.balancer.Remove(entry.handle)

	c.mu.Unlock()

	entry.conn.conn.Close()
}

// c.mu read lock must be held.
func (c *cluster) await() func() <-chan struct{} {
	prev := c.wait
	return func() <-chan struct{} {
		c.mu.RLock()
		wait := c.wait
		c.mu.RUnlock()
		if wait != prev {
			return wait
		}

		c.mu.Lock()
		wait = c.wait
		if wait != prev {
			c.mu.Unlock()
			return wait
		}
		wait = make(chan struct{})
		c.wait = wait
		c.mu.Unlock()

		return wait
	}
}

func compareEndpoints(a, b Endpoint) int {
	if c := strings.Compare(a.Addr, b.Addr); c != 0 {
		return c
	}
	if c := a.Port - b.Port; c != 0 {
		return c
	}
	return 0
}

func sortEndpoints(es []Endpoint) {
	sort.Slice(es, func(i, j int) bool {
		return compareEndpoints(es[i], es[j]) < 0
	})
}

func diffEndpoints(curr, next []Endpoint, eq, add, del func(i, j int)) {
	diffslice(
		len(curr),
		len(next),
		func(i, j int) int {
			return compareEndpoints(curr[i], next[j])
		},
		func(i, j int) {
			eq(i, j)
		},
		func(i, j int) {
			add(i, j)
		},
		func(i, j int) {
			del(i, j)
		},
	)
}

func diffslice(a, b int, cmp func(i, j int) int, eq, add, del func(i, j int)) {
	var i, j int
	for i < a && j < b {
		c := cmp(i, j)
		switch {
		case c < 0:
			del(i, j)
			i++
		case c > 0:
			add(i, j)
			j++
		default:
			eq(i, j)
			i++
			j++
		}
	}
	for ; i < a; i++ {
		del(i, j)
	}
	for ; j < b; j++ {
		add(i, j)
	}
}

type connListElement struct {
	index int
	conn  *conn
	info  connInfo
}

type connList []*connListElement

func (cs *connList) Insert(conn *conn, info connInfo) *connListElement {
	e := &connListElement{
		index: len(*cs),
		conn:  conn,
		info:  info,
	}
	*cs = append(*cs, e)
	return e
}

func (cs *connList) Remove(x *connListElement) {
	list := *cs
	var (
		n    = len(list)
		last = list[n-1]
	)
	last.index = x.index
	list[x.index], list[n-1] = list[n-1], nil
	list = list[:n-1]
	*cs = list
}
