package ydb

import (
	"container/list"
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/connectivity"

	"github.com/yandex-cloud/ydb-go-sdk/v2/timeutil"
)

const (
	MaxGetConnTimeout    = 10 * time.Second
	ConnResetOfflineRate = uint64(10)
)

var (
	// ErrClusterClosed returned when requested on a closed cluster.
	ErrClusterClosed = errors.New("cluster closed")

	// ErrClusterEmpty returned when no connections left in cluster.
	ErrClusterEmpty = errors.New("cluster empty")

	// ErrUnknownEndpoint returned when no connections left in cluster.
	ErrUnknownEndpoint = errors.New("unknown endpoint")
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
	explorer *repeater
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
		go c.tracker(timeutil.NewTimer(time.Duration(1<<63 - 1)))
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
	if c.explorer != nil {
		c.explorer.Stop()
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
			_ = cc.Close()
		}
	}

	<-c.trackerDone

	return
}

func (c *cluster) getNext(
	ctx context.Context,
) (
	conn *conn,
	closed bool,
	wait func() <-chan struct{},
	ready int,
	size int,
) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if endpointInfo := ContextEndpointInfo(ctx); endpointInfo != nil {
		if connEntry, ok := c.index[connAddrFromString(endpointInfo.Address())]; ok && isReady(connEntry.conn) {
			return connEntry.conn, c.closed, c.await(), c.ready, len(c.index)
		}
	}
	return c.balancer.Next(), c.closed, c.await(), c.ready, len(c.index)
}

// Get returns next available connection.
// It returns error on given context cancellation or when cluster become closed.
func (c *cluster) Get(ctx context.Context) (conn *conn, err error) {
	// Hard limit for get operation.
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, MaxGetConnTimeout)
	defer cancel()

	for {
		conn, closed, wait, ready, size := c.getNext(ctx)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			switch {
			case closed:
				return nil, ErrClusterClosed
			case ready == 0 && conn != nil:
				panic("ydb: empty balancer has returned non-nil conn")
			case ready != 0 && conn == nil:
				panic("ydb: non-empty balancer has returned nil conn")
			case size == 0:
				return nil, ErrClusterEmpty
			case conn != nil && isReady(conn):
				return conn, nil
			case conn != nil:
				c.mu.Lock()
				entry, has := c.index[conn.addr]
				if has && entry.handle != nil {
					// entry.handle may become nil when some race happened and other
					// goroutine already removed conn from balancer and sent it
					// to the tracker.
					conn.runtime.setState(ConnOffline)

					// NOTE: we're setting entry.conn to nil here to be more strict
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
					ready = c.ready

					// more than half connections under tracking - re-discover now
					if c.explorer != nil && ready*2 < len(c.index) {
						// emit signal for re-discovery
						c.explorer.Force()
					}
				}
				c.mu.Unlock()
			case ready <= 0:
				if c.explorer != nil {
					// emit signal for re-discovery
					c.explorer.Force()
				}
				select {
				// wait if no ready connections left
				case <-wait():

				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
		}
	}
}

func isReady(conn *conn) bool {
	return conn != nil && conn.conn != nil && conn.conn.GetState() == connectivity.Ready
}

func isBroken(conn *conn) bool {
	if conn == nil || conn.conn == nil {
		return true
	}
	state := conn.conn.GetState()
	return state == connectivity.Shutdown || state == connectivity.TransientFailure
}

// Insert inserts new connection into the cluster.
func (c *cluster) Insert(ctx context.Context, e Endpoint, wg ...WG) {
	if len(wg) > 0 {
		defer wg[0].Done()
	}

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
			_ = cc.Close()
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
	if isReady(conn) {
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
func (c *cluster) Update(ctx context.Context, ep Endpoint, wg ...WG) {
	if len(wg) > 0 {
		defer wg[0].Done()
	}

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
	if entry.conn != nil {
		entry.conn.runtime.setState(ConnOnline)
	}
	c.index[addr] = entry
	if entry.handle != nil {
		// entry.handle may be nil when connection is being tracked.
		c.balancer.Update(entry.handle, info)
	}
}

// Remove removes and closes previously inserted connection.
func (c *cluster) Remove(_ context.Context, e Endpoint, wg ...WG) {
	if len(wg) > 0 {
		defer wg[0].Done()
	}

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
		_ = entry.conn.conn.Close()
	}
}

func (c *cluster) Pessimize(addr connAddr) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClusterClosed
	}

	entry, has := c.index[addr]
	if !has {
		return ErrUnknownEndpoint
	}
	if entry.handle == nil {
		return ErrNilBalancerElement
	}
	if !c.balancer.Contains(entry.handle) {
		return ErrUnknownBalancerElement
	}
	err = c.balancer.Pessimize(entry.handle)
	if err == nil && c.explorer != nil {
		// count ratio (banned/all)
		online := 0
		for _, e := range c.index {
			if e.conn != nil && e.conn.runtime.getState() == ConnOnline {
				online++
			}
		}
		// more then half connections banned - re-discover now
		if online*2 < len(c.index) {
			c.explorer.Force()
		}
	}
	return err
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
	driverTraceTrackConnStart(context.Background(), c.trace, conn.addr.String())
	el = c.trackerQueue.PushBack(conn)
	select {
	case c.trackerWake <- struct{}{}:
	default:
	}
	return
}

func (c *cluster) tracker(timer timeutil.Timer) {
	defer close(c.trackerDone)

	var active bool
	if !timer.Stop() {
		panic("ydb: can't stop timer")
	}
	backoff := LogBackoff{
		SlotDuration: 5 * time.Millisecond,
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
				if conn.conn != nil && (isBroken(conn) || conn.runtime.offlineCount%ConnResetOfflineRate == 0) {
					co := conn.conn
					conn.conn = nil
					go func() { _ = co.Close() }()
				}

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

					if conn.runtime.getState() != ConnBanned {
						conn.runtime.setState(ConnOnline)
					}

					driverTraceTrackConnDone(context.Background(), c.trace, conn.addr.String())
					entry.conn = conn
					entry.insertInto(c.balancer)
					c.index[addr] = entry
					c.ready++

					wait = c.wait
					c.wait = nil
				}
				c.mu.Unlock()
				if !actual {
					_ = conn.conn.Close()
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
					_ = conn.conn.Close()
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

func (cs *connList) Contains(x *connListElement) bool {
	l := *cs
	var (
		n = len(l)
	)
	if x.index >= n {
		return false
	}
	return l[x.index] == x
}
