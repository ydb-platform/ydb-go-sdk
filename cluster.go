package ydb

import (
	"container/heap"
	"context"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

type connEntry struct {
	index int
	load  float32
}

type cluster struct {
	dial func(context.Context, string, int) (*conn, error)

	once   sync.Once
	up     sync.Mutex
	closed bool
	index  map[connAddr]connEntry
	lmin   float32
	lmax   float32

	mu    sync.RWMutex
	conns []*conn
	belt  []int
	next  int32
	wait  chan struct{}
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

	conns := c.conns
	c.conns = nil
	c.index = nil
	c.mu.Unlock()

	if wait != nil {
		close(wait)
	}
	for _, c := range conns {
		c.conn.Close()
	}

	return
}

func (c *cluster) Get(ctx context.Context) (conn *conn, err error) {
	for {
		c.mu.RLock()
		closed := c.closed
		wait := c.await()
		if n := len(c.conns); n > 0 {
			d := int(atomic.AddInt32(&c.next, 1)) % len(c.belt)
			i := c.belt[d]
			conn = c.conns[i]
		}
		c.mu.RUnlock()
		if conn != nil {
			return conn, nil
		}
		if closed {
			return nil, ErrClosed
		}
		select {
		case <-wait():
			// Continue.
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (c *cluster) Upsert(ctx context.Context, e Endpoint) (err error) {
	c.init()

	c.up.Lock()
	defer c.up.Unlock()
	if c.closed {
		return ErrClosed
	}

	c.mu.RLock()
	next := len(c.conns)
	c.mu.RUnlock()

	var (
		rebuild bool

		min  float32
		max  float32
		conn *conn
	)
	addr := connAddr{e.Addr, e.Port}
	x, has := c.index[addr]
	if !has {
		conn, err = c.dial(ctx, e.Addr, e.Port)
		if err != nil {
			return err
		}
		x.index = next
	}
	if min = c.lmin; len(c.index) == 0 || e.LoadFactor < min {
		min = e.LoadFactor
		rebuild = true
	}
	if max = c.lmax; len(c.index) == 0 || e.LoadFactor > max {
		max = e.LoadFactor
		rebuild = true
	}
	if !has || x.load != e.LoadFactor {
		x.load = e.LoadFactor
		c.index[addr] = x
		rebuild = true
	}
	if !rebuild {
		return nil
	}

	c.lmin = min
	c.lmax = max
	belt := c.distribute()

	c.mu.Lock()
	c.conns = append(c.conns, conn)
	c.belt = belt

	wait := c.wait
	c.wait = nil
	c.mu.Unlock()

	if wait != nil {
		close(wait)
	}

	return nil
}

// c.up must be held.
func (c *cluster) distribute() []int {
	return c.spread(distribution(
		c.lmin, int32(len(c.conns)),
		c.lmax, 1,
	))
}

// c.up must be held.
func (c *cluster) spread(f func(float32) int32) []int {
	dist := make([]int32, 0, len(c.index))
	index := make([]int, 0, len(c.index))
	for _, x := range c.index {
		d := f(x.load)
		dist = append(dist, d)
		index = append(index, x.index)
	}
	return genBelt(index, dist)
}

func (c *cluster) Remove(addr connAddr) bool {
	c.up.Lock()
	defer c.up.Unlock()
	if c.closed {
		return false
	}
	e, ok := c.index[addr]
	if !ok {
		return false
	}

	c.mu.RLock()
	n := len(c.conns)
	next := c.conns[n-1]
	c.mu.RUnlock()

	load := e.load
	var (
		min     float32
		max     float32
		inspect bool
	)
	if min = c.lmin; load == min {
		inspect = true
	}
	if max = c.lmax; load == max {
		inspect = true
	}

	delete(c.index, addr)
	if next.addr != addr {
		nx := c.index[next.addr]
		nx.index = e.index
		c.index[next.addr] = nx
	}

	if !inspect {
		var def bool
		for _, e := range c.index {
			load := e.load
			if !def {
				min = load
				max = load
				def = true
			}
			if load < min {
				min = load
			}
			if load > max {
				max = load
			}
		}
		c.lmin = min
		c.lmax = max
	}

	belt := c.distribute()

	c.mu.Lock()
	c.conns[e.index], c.conns[n-1] = c.conns[n-1], nil
	c.conns = c.conns[:n-1]
	c.belt = belt
	c.mu.Unlock()

	return true
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

func distribution(x1 float32, y1 int32, x2 float32, y2 int32) (f func(float32) int32) {
	if x1 == x2 {
		f = func(float32) int32 { return 1 }
	} else {
		a := float32(y2-y1) / (x2 - x1)
		b := float32(y1) - a*x1
		f = func(x float32) int32 {
			return int32(math.Round(float64(a*x + b)))
		}
	}
	return f
}

type distItem struct {
	i   int
	stp float64
	val float64
}

// newDistItem creates new distribution item.
// w must be greater than zero.
func newDistItem(i int, w int32) *distItem {
	stp := 1 / float64(w)
	return &distItem{
		i:   i,
		stp: stp,
		val: stp,
	}
}

func (x *distItem) tick() bool {
	x.val += x.stp
	return x.val <= 1
}

func (x *distItem) index() int {
	return x.i
}

type distItemsHeap []*distItem

func (h distItemsHeap) Len() int           { return len(h) }
func (h distItemsHeap) Less(i, j int) bool { return h[i].val < h[j].val }
func (h distItemsHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *distItemsHeap) Push(x interface{}) {
	*h = append(*h, x.(*distItem))
}

func (h *distItemsHeap) Pop() interface{} {
	p := *h
	n := len(p)
	x := p[n-1]
	*h = p[:n-1]
	return x
}

func genBelt(index []int, weight []int32) (r []int) {
	h := make(distItemsHeap, len(weight))
	for i, w := range weight {
		h[i] = newDistItem(index[i], w)
	}
	heap.Init(&h)
	for len(h) > 0 {
		x := heap.Pop(&h).(*distItem)
		r = append(r, x.index())
		if x.tick() {
			heap.Push(&h, x)
		}
	}
	return
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
