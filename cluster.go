package ydb

import (
	"container/list"
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/connectivity"

	"github.com/yandex-cloud/ydb-go-sdk/timeutil"
)

// connInfo contains connection "static" stats – e.g. such that obtained from
// discovery routine.
type connInfo struct {
	loadFactor float32
	local      bool
}

// connEntry represents inserted into the cluster connection.
type connEntry struct {
	conn           *conn
	handle         balancerElement
	trackerQueueEl *list.Element

	info connInfo
}

func (c *connEntry) insertInto(b balancer) {
	if c.handle != nil {
		panic("ydb: handle already exists")
	}
	if c.conn == nil {
		panic("ydb: can't insert nil conn into balancer")
	}
	c.handle = b.Insert(c.conn, c.info)
	if c.handle == nil {
		panic("ydb: balancer has returned nil handle")
	}
}

func (c *connEntry) removeFrom(b balancer) {
	if c.handle == nil {
		panic("ydb: no handle to remove from balancer")
	}
	b.Remove(c.handle)
	c.handle = nil
}

type cluster struct {
	dial     func(context.Context, string, int) (*conn, error)
	balancer balancer
	trace    DriverTrace

	mu    sync.RWMutex
	once  sync.Once
	index map[connAddr]connEntry
	ready int
	wait  chan struct{}

	trackerCtx    context.Context
	trackerCancel context.CancelFunc
	trackerWake   chan struct{}
	trackerDone   chan struct{}
	trackerQueue  *list.List // list of *conn.

	closed bool

	testHookTrackerQueue func([]*list.Element)
}

func (c *cluster) init() {
	c.once.Do(func() {
		c.index = make(map[connAddr]connEntry)

		c.trackerCtx, c.trackerCancel = context.WithCancel(context.Background())
		c.trackerWake = make(chan struct{}, 1)
		c.trackerDone = make(chan struct{})
		c.trackerQueue = list.New()
		go c.tracker()
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

	c.trackerCancel()

	c.mu.Unlock()

	if wait != nil {
		close(wait)
	}
	for _, entry := range index {
		c := entry.conn
		if c == nil {
			continue
		}
		if cc := c.conn; cc != nil {
			cc.Close()
		}
	}

	<-c.trackerDone

	return
}

// Get returns next available connection.
// It returns error on given context cancelation or when cluster become closed.
func (c *cluster) Get(ctx context.Context) (conn *conn, err error) {
	for {
		c.mu.RLock()
		closed := c.closed
		wait := c.await()
		size := c.ready
		conn = c.balancer.Next()
		c.mu.RUnlock()
		if closed {
			return nil, ErrClosed
		}
		switch {
		case size == 0 && conn != nil:
			panic("ydb: empty balancer has returned non-nil conn")
		case size != 0 && conn == nil:
			panic("ydb: non-empty balancer has returned nil conn")
		case conn != nil:
			if isReady(conn) {
				return conn, nil
			}
			c.mu.Lock()
			entry, has := c.index[conn.addr]
			if !has {
				panic("ydb: balancer returned non-indexed conn")
			}
			if entry.handle != nil {
				// entry.handle may become nil when some race happened and other
				// goroutine already removed conn from balancer and sent it
				// to the tracker.
				conn.runtime.setState(ConnOffline)

				// NOTE: we setting entry.conn to nil here to be more strict
				// about the ownership of conn. That is, tracker goroutine
				// takes full ownership of conn after c.track(conn) call.
				//
				// Leaving non-nil conn may lead to data races, when tracker
				// changes conn.conn field (in case of unsuccessful initial
				// dial) without any mutex used.
				entry.removeFrom(c.balancer)
				entry.conn = nil
				entry.trackerQueueEl = c.track(conn)

				c.index[conn.addr] = entry
				c.ready--
			}
			c.mu.Unlock()
		}
		select {
		case <-wait():
			// Continue.
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func isReady(conn *conn) bool {
	return conn.conn != nil && conn.conn.GetState() == connectivity.Ready
}

// Insert inserts new connection into the cluster.
func (c *cluster) Insert(ctx context.Context, e Endpoint) {
	c.init()

	addr := connAddr{e.Addr, e.Port}
	info := connInfo{
		loadFactor: e.LoadFactor,
		local:      e.Local,
	}
	conn, err := c.dial(ctx, e.Addr, e.Port)
	if err != nil {
		conn = newConn(nil, addr)
		err = nil
	}
	cc := conn.conn
	var wait chan struct{}
	defer func() {
		if err != nil && cc != nil {
			cc.Close()
			return
		}
		if wait != nil {
			close(wait)
		}
	}()

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		err = ErrClosed
		return
	}
	_, has := c.index[addr]
	if has {
		panic("ydb: can't insert already existing endpoint")
	}
	entry := connEntry{info: info}
	if cc != nil {
		conn.runtime.setState(ConnOnline)
		entry.conn = conn
		entry.insertInto(c.balancer)
		c.ready++
		wait = c.wait
		c.wait = nil
	} else {
		conn.runtime.setState(ConnOffline)
		entry.trackerQueueEl = c.track(conn)
	}
	c.index[addr] = entry
}

// Update updates existing connection's runtime stats such that load factor and
// others.
func (c *cluster) Update(ctx context.Context, ep Endpoint) {
	addr := connAddr{ep.Addr, ep.Port}
	info := connInfo{
		loadFactor: ep.LoadFactor,
		local:      ep.Local,
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}

	entry, has := c.index[addr]
	if !has {
		panic("ydb: can't update not-existing endpoint")
	}

	entry.info = info
	c.index[addr] = entry
	if entry.handle != nil {
		// entry.handle may be nil when connection is being tracked.
		c.balancer.Update(entry.handle, info)
	}
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
		panic("ydb: can't remove not-existing endpoint")
	}
	if el := entry.trackerQueueEl; el != nil {
		// Connection is being tracked.
		c.trackerQueue.Remove(el)
	} else {
		entry.removeFrom(c.balancer)
		c.ready--
	}
	delete(c.index, addr)
	c.mu.Unlock()

	if entry.conn != nil {
		// entry.conn may be nil when connection is being tracked after
		// unsuccessful dial().
		entry.conn.conn.Close()
	}
}

func (c *cluster) Stats(it func(Endpoint, ConnStats)) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return
	}
	call := func(conn *conn, info connInfo) {
		e := Endpoint{
			Addr:       conn.addr.addr,
			Port:       conn.addr.port,
			LoadFactor: info.loadFactor,
			Local:      info.local,
		}
		s := conn.runtime.stats()
		it(e, s)
	}
	for el := c.trackerQueue.Front(); el != nil; el = el.Next() {
		conn := el.Value.(*conn)
		entry := c.index[conn.addr]
		call(conn, entry.info)
	}
	for _, entry := range c.index {
		if entry.conn != nil {
			call(entry.conn, entry.info)
		}
	}
}

// c.mu must be held.
func (c *cluster) track(conn *conn) (el *list.Element) {
	c.trace.trackConnStart(conn)
	el = c.trackerQueue.PushBack(conn)
	c.wakeUpTracker()
	return
}

func (c *cluster) wakeUpTracker() {
	select {
	case c.trackerWake <- struct{}{}:
	default:
	}
}

func (c *cluster) tracker() {
	defer close(c.trackerDone)

	var active bool
	timer := timeutil.NewTimer(time.Duration(1<<63 - 1))
	if !timer.Stop() {
		panic("ydb: can't stop timer")
	}
	backoff := LogBackoff{
		SlotDuration: time.Millisecond,
		Ceiling:      10, // ~1s (2^10ms)
		JitterLimit:  1,  // Without randomization.
	}

	var queue []*list.Element
	fetchQueue := func(dest []*list.Element) []*list.Element {
		c.mu.RLock()
		defer c.mu.RUnlock()
		for el := c.trackerQueue.Front(); el != nil; el = el.Next() {
			dest = append(dest, el)
		}
		return dest
	}
	for i := 0; ; i++ {
		select {
		case <-c.trackerWake:
			if active && !timer.Stop() {
				<-timer.C()
			}
			i = 0
			timer.Reset(backoff.Delay(i))

		case <-timer.C():
			queue = fetchQueue(queue[:0])
			active = len(queue) > 0

			if f := c.testHookTrackerQueue; f != nil {
				f(queue)
			}

			ctx, cancel := context.WithTimeout(c.trackerCtx, time.Second)
			for _, el := range queue {
				conn := el.Value.(*conn)
				addr := conn.addr
				if conn.conn == nil {
					x, err := c.dial(ctx, addr.addr, addr.port)
					if err == nil {
						conn.conn = x.conn
					}
				}
				if !isReady(conn) {
					continue
				}

				var wait chan struct{}
				c.mu.Lock()
				entry, has := c.index[addr]
				actual := has && entry.trackerQueueEl == el
				if actual {
					// Element is still in the index and element is actual.
					//
					// NOTE: we are checking both `has` flag and equality with
					// `entry.trackerQueueEl` to get rid of races, when
					// endpoint removed and added immediately while we stuck on
					// dialing above.
					c.trackerQueue.Remove(el)
					active = c.trackerQueue.Len() > 0

					conn.runtime.setState(ConnOnline)
					c.trace.trackConnDone(conn)
					entry.conn = conn
					entry.insertInto(c.balancer)
					c.index[addr] = entry
					c.ready++

					wait = c.wait
					c.wait = nil
				}
				c.mu.Unlock()
				if !actual {
					conn.conn.Close()
				}
				if wait != nil {
					close(wait)
				}
			}
			cancel()
			if active {
				timer.Reset(backoff.Delay(i))
			}

		case <-c.trackerCtx.Done():
			queue = fetchQueue(queue[:0])
			for _, el := range queue {
				conn := el.Value.(*conn)
				if conn.conn != nil {
					conn.conn.Close()
				}
			}
			return
		}
	}
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

type singleConnBalancer struct {
	conn *conn
}

func (s *singleConnBalancer) Next() *conn {
	return s.conn
}
func (s *singleConnBalancer) Insert(conn *conn, _ connInfo) balancerElement {
	if s.conn != nil {
		panic("ydb: single conn balancer: double Insert()")
	}
	s.conn = conn
	return conn
}
func (s *singleConnBalancer) Remove(el balancerElement) {
	if s.conn != el.(*conn) {
		panic("ydb: single conn balancer: Remove() unknown conn")
	}
	s.conn = nil
}
func (s *singleConnBalancer) Update(balancerElement, connInfo) {}
