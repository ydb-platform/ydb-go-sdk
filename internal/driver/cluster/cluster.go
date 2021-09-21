package cluster

import (
	"container/list"
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	public "github.com/ydb-platform/ydb-go-sdk/v3/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/entry"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/info"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/runtime/stats"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/runtime/stats/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/repeater"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/wg"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

const (
	MaxGetConnTimeout = 10 * time.Second
	//ConnResetOfflineRate = uint64(10)
)

var (
	// ErrClusterClosed returned when requested on a closed cluster.
	ErrClusterClosed = errors.New("cluster closed")

	// ErrClusterEmpty returned when no connections left in cluster.
	ErrClusterEmpty = errors.New("cluster empty")

	// ErrUnknownEndpoint returned when no connections left in cluster.
	ErrUnknownEndpoint = errors.New("unknown endpoint")
)

type cluster struct {
	dial     func(context.Context, string, int) (*grpc.ClientConn, error)
	balancer balancer.Balancer
	explorer repeater.Repeater

	mu    sync.RWMutex
	once  sync.Once
	index map[public.Addr]entry.Entry
	ready int
	wait  chan struct{}

	//trackerCtx    context.Context
	//trackerCancel context.CancelFunc
	//trackerWake   chan struct{}
	//trackerDone   chan struct{}
	//trackerQueue  *list.List // list of *Conn.

	closed bool

	testHookTrackerQueue func([]*list.Element)
	// tracer function. Set after the call driverTraceTrackConnStart .
	trackDone func(trace.TrackConnDoneInfo)
}

func (c *cluster) Force() {
	c.explorer.Force()
}

func (c *cluster) SetExplorer(repeater repeater.Repeater) {
	c.explorer = repeater
}

type Cluster interface {
	Insert(ctx context.Context, e public.Endpoint, opts ...option)
	Update(ctx context.Context, e public.Endpoint, opts ...option)
	Get(ctx context.Context) (conn conn.Conn, err error)
	Pessimize(a public.Addr) error
	Stats(it func(public.Endpoint, stats.Stats))
	Close() error
	Remove(ctx context.Context, e public.Endpoint, wg ...option)
	SetExplorer(repeater repeater.Repeater)
	Force()
}

func New(
	dial func(context.Context, string, int) (*grpc.ClientConn, error),
	balancer balancer.Balancer,
) Cluster {
	return &cluster{
		index:    make(map[public.Addr]entry.Entry),
		dial:     dial,
		balancer: balancer,
	}
}

//func (c *cluster) init() {
//	c.once.Do(func() {
//		c.index = make(map[addr.Host]Entry)
//
//		c.trackerCtx, c.trackerCancel = context.WithCancel(context.Background())
//		c.trackerWake = make(chan struct{}, 1)
//		c.trackerDone = make(chan struct{})
//		c.trackerQueue = list.New()
//		//go c.tracker(timeutil.NewTimer(time.Duration(1<<63 - 1)))
//	})
//}

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

	//c.trackerCancel()
	//
	c.mu.Unlock()

	if wait != nil {
		close(wait)
	}
	for _, entry := range index {
		conn := entry.Conn
		if conn == nil {
			continue
		}
		_ = conn.Close()
	}

	//<-c.trackerDone

	return
}

// Get returns next available connection.
// It returns error on given context cancellation or when cluster become closed.
func (c *cluster) Get(ctx context.Context) (conn conn.Conn, err error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return nil, ErrClusterClosed
	}
	conn = c.balancer.Next()
	if conn == nil {
		return nil, ErrClusterEmpty
	}
	return conn, nil
	//
	//// Hard limit for get operation.
	//var cancel context.CancelFunc
	//ctx, cancel = context.WithTimeout(ctx, MaxGetConnTimeout)
	//defer cancel()
	//
	//for {
	//	c.mu.RLock()
	//	closed := c.closed
	//	wait := c.await()
	//	ready := c.ready
	//	size := len(c.index)
	//	Conn = c.balancer.Next()
	//	c.mu.RUnlock()
	//	select {
	//	case <-ctx.Done():
	//		return nil, ctx.err()
	//	default:
	//		switch {
	//		case closed:
	//			return nil, ErrClusterClosed
	//		case ready == 0 && Conn != nil:
	//			panic("ydb: empty balancer has returned non-nil Conn")
	//		case ready != 0 && Conn == nil:
	//			panic("ydb: non-empty balancer has returned nil Conn")
	//		case size == 0:
	//			return nil, ErrClusterEmpty
	//		case Conn != nil && Conn.IsReady():
	//			return Conn, nil
	//		case Conn != nil:
	//			c.mu.Lock()
	//			entry, has := c.index[Conn.addr]
	//			if has && entry.Handle != nil {
	//				// entry.Handle may become nil when some race happened and other
	//				// goroutine already removed Conn from balancer and sent it
	//				// to the tracker.
	//				Conn.runtime.setState(Offline)
	//
	//				// NOTE: we setting entry.Conn to nil here to be more strict
	//				// about the ownership of Conn. That is, tracker goroutine
	//				// takes full ownership of Conn after c.track(Conn) call.
	//				//
	//				// Leaving non-nil Conn may lead to data races, when tracker
	//				// changes Conn.Conn field (in case of unsuccessful initial
	//				// dial) without any mutex used.
	//				entry.removeFrom(c.balancer)
	//				entry.Conn = nil
	//				entry.TrackerQueueEl = c.track(Conn)
	//
	//				c.index[Conn.addr] = entry
	//				c.ready--
	//				ready = c.ready
	//
	//				// more then half connections under tracking - re-discover now
	//				if c.explorer != nil && ready*2 < len(c.index) {
	//					c.explorer.Force()
	//				}
	//			}
	//			c.mu.Unlock()
	//		case ready <= 0:
	//			select {
	//			// wait if no ready connections left
	//			case <-wait():
	//
	//			case <-ctx.Done():
	//				return nil, ctx.err()
	//			}
	//		}
	//	}
	//}
}

type options struct {
	wg         wg.WG
	connConfig conn.Config
}

type option func(options *options)

func WithWG(wg wg.WG) option {
	return func(options *options) {
		options.wg = wg
	}
}

func WithConnConfig(connConfig conn.Config) option {
	return func(options *options) {
		options.connConfig = connConfig
	}
}

// Insert inserts new connection into the cluster.
func (c *cluster) Insert(ctx context.Context, e public.Endpoint, opts ...option) {
	opt := options{}
	for _, o := range opts {
		o(&opt)
	}
	if opt.wg != nil {
		defer opt.wg.Done()
	}

	//c.init()
	//
	addr := public.Addr{e.Host, e.Port}
	info := info.Info{
		LoadFactor: e.LoadFactor,
		Local:      e.Local,
	}
	conn := conn.New(addr, c.dial, opt.connConfig)
	var wait chan struct{}
	defer func() {
		if wait != nil {
			close(wait)
		}
	}()

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return
	}
	_, has := c.index[addr]
	if has {
		panic("ydb: can't insert already existing endpoint")
	}
	entry := entry.Entry{Info: info}
	entry.Conn = conn
	entry.InsertInto(c.balancer)
	c.ready++
	wait = c.wait
	c.wait = nil
	c.index[addr] = entry
}

// Update updates existing connection's runtime stats such that load factor and
// others.
func (c *cluster) Update(_ context.Context, ep public.Endpoint, opts ...option) {
	opt := options{}
	for _, o := range opts {
		o(&opt)
	}
	if opt.wg != nil {
		defer opt.wg.Done()
	}

	addr := public.Addr{ep.Host, ep.Port}
	info := info.Info{
		LoadFactor: ep.LoadFactor,
		Local:      ep.Local,
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

	entry.Info = info
	if entry.Conn != nil {
		entry.Conn.Runtime().SetState(state.Online)
	}
	c.index[addr] = entry
	if entry.Handle != nil {
		// entry.Handle may be nil when connection is being tracked.
		c.balancer.Update(entry.Handle, info)
	}
}

// Remove removes and closes previously inserted connection.
func (c *cluster) Remove(_ context.Context, e public.Endpoint, opts ...option) {
	opt := options{}
	for _, o := range opts {
		o(&opt)
	}
	if opt.wg != nil {
		defer opt.wg.Done()
	}

	addr := public.Addr{e.Host, e.Port}

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
	//if el := entry.TrackerQueueEl; el != nil {
	//	// Connection is being tracked.
	//	c.trackerQueue.Remove(el)
	//} else {
	entry.RemoveFrom(c.balancer)
	c.ready--
	//}
	delete(c.index, addr)
	c.mu.Unlock()

	if entry.Conn != nil {
		// entry.Conn may be nil when connection is being tracked after
		// unsuccessful dial().
		_ = entry.Conn.Close()
	}
}

func (c *cluster) Pessimize(addr public.Addr) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClusterClosed
	}

	entry, has := c.index[addr]
	if !has {
		return ErrUnknownEndpoint
	}
	if entry.Handle == nil {
		return balancer.ErrNilBalancerElement
	}
	if !c.balancer.Contains(entry.Handle) {
		return balancer.ErrUnknownBalancerElement
	}
	err = c.balancer.Pessimize(entry.Handle)
	if err == nil && c.explorer != nil {
		// count ratio (banned/all)
		online := 0
		for _, e := range c.index {
			if e.Conn != nil && e.Conn.Runtime().GetState() == state.Online {
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

func (c *cluster) Stats(it func(public.Endpoint, stats.Stats)) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return
	}
	call := func(conn conn.Conn, info info.Info) {
		e := public.Endpoint{
			Addr: public.Addr{
				Host: conn.Addr().Host,
				Port: conn.Addr().Port,
			},
			LoadFactor: info.LoadFactor,
			Local:      info.Local,
		}
		s := conn.Runtime().Stats()
		it(e, s)
	}
	//for el := c.trackerQueue.Front(); el != nil; el = el.Next() {
	//	conn := el.Value.(conn.Conn)
	//	entry := c.index[conn.Host()]
	//	call(conn, entry.Info)
	//}
	for _, entry := range c.index {
		if entry.Conn != nil {
			call(entry.Conn, entry.Info)
		}
	}
}

//// c.mu must be held.
//func (c *cluster) track(conn conn.Conn) (el *list.Element) {
//	c.trackDone = c.sessiontrace.OnTrackConn(sessiontrace.TrackConnStartInfo{
//		Address: conn.Host().String(),
//	})
//	el = c.trackerQueue.PushBack(conn)
//	select {
//	case c.trackerWake <- struct{}{}:
//	default:
//	}
//	return
//}

//func (c *cluster) tracker(timer timeutil.Timer) {
//	defer close(c.trackerDone)
//
//	var active bool
//	if !timer.Stop() {
//		panic("ydb: can't stop timer")
//	}
//	backoff := logBackoff{
//		SlotDuration: 5 * time.Millisecond,
//		Ceiling:      10, // ~1s (2^10ms)
//		JitterLimit:  1,  // Without randomization.
//	}
//
//	var queue []*list.Element
//	fetchQueue := func(dest []*list.Element) []*list.Element {
//		c.mu.RLock()
//		defer c.mu.RUnlock()
//		for el := c.trackerQueue.Front(); el != nil; el = el.Next() {
//			dest = append(dest, el)
//		}
//		return dest
//	}
//	for i := 0; ; i++ {
//		select {
//		case <-c.trackerWake:
//			if active && !timer.Stop() {
//				<-timer.C()
//			}
//			i = 0
//			timer.Reset(backoff.delay(i))
//
//		case <-timer.C():
//			queue = fetchQueue(queue[:0])
//			active = len(queue) > 0
//
//			if f := c.testHookTrackerQueue; f != nil {
//				f(queue)
//			}
//
//			ctx, cancel := context.WithTimeout(c.trackerCtx, time.Second)
//			for _, el := range queue {
//				Conn := el.Value.(*Conn)
//				if Conn.raw != nil && (isBroken(Conn) || Conn.runtime.offlineCount%ConnResetOfflineRate == 0) {
//					co := Conn.raw
//					Conn.raw = nil
//					go func() { _ = co.Close() }()
//				}
//
//				addr := Conn.addr
//				if Conn.raw == nil {
//					x, err := c.dial(ctx, addr.addr, addr.port)
//					if err == nil {
//						Conn.raw = x.raw
//					}
//				}
//				if !IsReady(Conn) {
//					continue
//				}
//
//				var wait chan struct{}
//				c.mu.Lock()
//				entry, has := c.index[addr]
//				actual := has && entry.TrackerQueueEl == el
//				if actual {
//					// Element is still in the index and element is actual.
//					//
//					// NOTE: we are checking both `has` flag and equality with
//					// `entry.TrackerQueueEl` to get rid of races, when
//					// endpoint removed and added immediately while we stuck on
//					// dialing above.
//					c.trackerQueue.Remove(el)
//					active = c.trackerQueue.Len() > 0
//
//					if Conn.runtime.getState() != Banned {
//						Conn.runtime.setState(Online)
//					}
//
//					c.trackDone(Conn.addr.String())
//					entry.Conn = Conn
//					entry.insertInto(c.balancer)
//					c.index[addr] = entry
//					c.ready++
//
//					wait = c.wait
//					c.wait = nil
//				}
//				c.mu.Unlock()
//				if !actual {
//					_ = Conn.raw.Close()
//				}
//				if wait != nil {
//					close(wait)
//				}
//			}
//			cancel()
//			if active {
//				timer.Reset(backoff.delay(i))
//			}
//
//		case <-c.trackerCtx.Done():
//			queue = fetchQueue(queue[:0])
//			for _, el := range queue {
//				Conn := el.Value.(*Conn)
//				if Conn.raw != nil {
//					_ = Conn.raw.Close()
//				}
//			}
//			return
//		}
//	}
//}

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

func compareEndpoints(a, b public.Endpoint) int {
	if c := strings.Compare(a.Host, b.Host); c != 0 {
		return c
	}
	if c := a.Port - b.Port; c != 0 {
		return c
	}
	return 0
}

func SortEndpoints(es []public.Endpoint) {
	sort.Slice(es, func(i, j int) bool {
		return compareEndpoints(es[i], es[j]) < 0
	})
}

func DiffEndpoints(curr, next []public.Endpoint, eq, add, del func(i, j int)) {
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
