package cluster

import (
	"container/list"
	"context"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	cluster2 "github.com/ydb-platform/ydb-go-sdk/v3/cluster/balancer"
	conn2 "github.com/ydb-platform/ydb-go-sdk/v3/cluster/balancer/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/cluster/balancer/conn/entry"
	"github.com/ydb-platform/ydb-go-sdk/v3/cluster/balancer/conn/info"
	list2 "github.com/ydb-platform/ydb-go-sdk/v3/cluster/balancer/conn/list"
	"github.com/ydb-platform/ydb-go-sdk/v3/cluster/balancer/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/cluster/balancer/conn/stats"
	"github.com/ydb-platform/ydb-go-sdk/v3/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/endpoint"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/ydb-platform/ydb-go-sdk/v3/timeutil"
	"github.com/ydb-platform/ydb-go-sdk/v3/timeutil/timetest"
)

func TestClusterFastRedial(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ln := newStubListener()
	srv := grpc.NewServer()
	go func() {
		_ = srv.Serve(ln)
	}()

	cs, balancer := simpleBalancer()
	c := &cluster{
		dial: func(ctx context.Context, s string, p int) (*conn.conn, error) {
			cc, err := ln.Dial(ctx)
			return &conn.conn{
				addr: conn.connAddr{s, p},
				raw:  cc,
			}, err
		},
		balancer: balancer,
	}

	pingConnects := func(size int) chan struct{} {
		done := make(chan struct{})
		go func() {
			for i := 0; i < size*10; i++ {
				con, err := c.Get(context.Background())
				// enforce close bad connects to track them
				if err == nil && con != nil && con.addr.addr == "bad" {
					_ = con.raw.Close()
				}
			}
			close(done)
		}()
		return done
	}

	ne := []endpoint.Endpoint{
		{Addr: "foo"},
		{Addr: "bad"},
	}
	mergeEndpointIntoCluster(ctx, c, []endpoint.Endpoint{}, ne)
	select {
	case <-pingConnects(len(ne)):

	case <-time.After(time.Second * 10):
		t.Fatalf("Time limit exceeded while %d endpoints in balance. Wait channel used", len(*cs))
	}
}

func withDisabledTrackerQueue(c *cluster) *cluster {
	c.index = make(map[conn.connAddr]entry.Entry)
	c.trackerQueue = list.New()
	c.once.Do(func() {})
	return c
}

func TestClusterMergeEndpoints(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ln := newStubListener()
	srv := grpc.NewServer()
	go func() {
		_ = srv.Serve(ln)
	}()

	cs, balancer := simpleBalancer()
	c := withDisabledTrackerQueue(&cluster{
		dial: func(ctx context.Context, s string, p int) (*conn.conn, error) {
			cc, err := ln.Dial(ctx)
			return &conn.conn{
				addr: conn.connAddr{s, p},
				raw:  cc,
			}, err
		},
		balancer: balancer,
	})

	pingConnects := func(size int) {
		for i := 0; i < size*10; i++ {
			sub, cancel := context.WithTimeout(ctx, time.Millisecond)
			defer cancel()
			con, err := c.Get(sub)
			// enforce close bad connects to track them
			if err == nil && con != nil && con.addr.addr == "bad" {
				_ = con.raw.Close()
			}
		}
	}
	assert := func(t *testing.T, total, inBalance, onTracking int) {
		if len(c.index) != total {
			t.Fatalf("total expected number of endpoints %d got %d", total, len(c.index))
		}
		if len(*cs) != inBalance {
			t.Fatalf("inBalance expected number of endpoints %d got %d", inBalance, len(*cs))
		}
		if c.trackerQueue.Len() != onTracking {
			t.Fatalf("onTracking expected number of endpoints %d got %d", onTracking, c.trackerQueue.Len())
		}
	}

	endpoints := []endpoint.Endpoint{
		{Addr: "foo"},
		{Addr: "foo", Port: 123},
	}
	badEndpoints := []endpoint.Endpoint{
		{Addr: "bad"},
		{Addr: "bad", Port: 123},
	}
	nextEndpoints := []endpoint.Endpoint{
		{Addr: "foo"},
		{Addr: "bar"},
		{Addr: "bar", Port: 123},
	}
	nextBadEndpoints := []endpoint.Endpoint{
		{Addr: "bad", Port: 23},
	}
	t.Run("initial fill", func(t *testing.T) {
		ne := append(endpoints, badEndpoints...)
		// merge new endpoints into balancer
		mergeEndpointIntoCluster(ctx, c, []endpoint.Endpoint{}, ne)
		// try endpoints, filter out bad ones to tracking
		pingConnects(len(ne))
		assert(t, len(ne), len(endpoints), len(badEndpoints))
	})
	t.Run("update with another endpoints", func(t *testing.T) {
		ne := append(nextEndpoints, nextBadEndpoints...)
		// merge new endpoints into balancer
		mergeEndpointIntoCluster(ctx, c, append(endpoints, badEndpoints...), ne)
		// try endpoints, filter out bad ones to tracking
		pingConnects(len(ne))
		assert(t, len(ne), len(nextEndpoints), len(nextBadEndpoints))
	})
	t.Run("left only bad", func(t *testing.T) {
		ne := nextBadEndpoints
		// merge new endpoints into balancer
		mergeEndpointIntoCluster(ctx, c, append(nextEndpoints, nextBadEndpoints...), ne)
		// try endpoints, filter out bad ones to tracking
		pingConnects(len(ne))
		assert(t, len(ne), 0, len(nextBadEndpoints))
	})
	t.Run("left only good", func(t *testing.T) {
		ne := nextEndpoints
		// merge new endpoints into balancer
		mergeEndpointIntoCluster(ctx, c, nextBadEndpoints, ne)
		// try endpoints, filter out bad ones to tracking
		pingConnects(len(ne))
		assert(t, len(ne), len(nextEndpoints), 0)
	})
}

func TestClusterRemoveTracking(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ln := newStubListener()
	srv := grpc.NewServer()
	go func() {
		_ = srv.Serve(ln)
	}()

	_, balancer := simpleBalancer()

	// Prevent tracker timer from firing.
	timer := timetest.StubSingleTimer(t)
	defer timer.Cleanup()

	tracking := make(chan int)
	defer close(tracking)
	assertTracking := func(exp int) {
		// Force tracker to collect the connections to track.
		timer.C <- timeutil.Now()
		if act := <-tracking; act != exp {
			t.Fatalf(
				"unexpected number of conns to track: %d; want %d",
				act, exp,
			)
		}
	}

	c := &cluster{
		dial: func(ctx context.Context, s string, p int) (*conn.conn, error) {
			cc, err := ln.Dial(ctx)
			return &conn.conn{
				addr: conn.connAddr{s, p},
				raw:  cc,
			}, err
		},
		balancer: balancer,
		testHookTrackerQueue: func(q []*list.Element) {
			tracking <- len(q)
		},
	}

	endpoint := endpoint.Endpoint{Addr: "foo"}
	c.Insert(ctx, endpoint)

	// Await for connection to be established.
	// Note that this is server side half.
	conn := <-ln.S

	// Do not accept new connections.
	_ = ln.Close()
	// Force cluster to reconnect.
	_ = conn.Close()
	// Await for Conn change its state inside cluster.
	{
		sub, cancel := context.WithTimeout(ctx, time.Millisecond)
		defer cancel()
		for {
			_, err := c.Get(sub)
			if err != nil {
				break
			}
		}
	}
	<-timer.Reset

	assertTracking(1)
	<-timer.Reset

	c.Remove(ctx, endpoint)

	assertTracking(0)
}

func TestClusterRemoveOffline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, balancer := simpleBalancer()

	// Prevent tracker timer from firing.
	timer := timetest.StubSingleTimer(t)
	defer timer.Cleanup()

	tracking := make(chan int)
	defer close(tracking)

	c := &cluster{
		dial: func(ctx context.Context, s string, p int) (*conn.conn, error) {
			return nil, fmt.Errorf("refused")
		},
		balancer: balancer,
		testHookTrackerQueue: func(q []*list.Element) {
			tracking <- len(q)
		},
	}

	endpoint := endpoint.Endpoint{Addr: "foo"}
	c.Insert(ctx, endpoint)
	<-timer.Reset

	c.Remove(ctx, endpoint)

	timer.C <- timeutil.Now()
	if n := <-tracking; n != 0 {
		t.Fatalf("unexpected %d tracking connection(s)", n)
	}
}

func TestClusterRemoveAndInsert(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ln := newStubListener()
	srv := grpc.NewServer()
	go func() {
		_ = srv.Serve(ln)
	}()

	_, balancer := simpleBalancer()

	// Prevent tracker timer from firing.
	timer := timetest.StubSingleTimer(t)
	defer timer.Cleanup()

	tracking := make(chan (<-chan int))
	defer close(tracking)
	assertTracking := func(exp int) {
		// Force tracker to collect the connections to track.
		timer.C <- timeutil.Now()
		ch := <-tracking
		if act := <-ch; act != exp {
			t.Fatalf(
				"unexpected number of conns to track: %d; want %d",
				act, exp,
			)
		}
	}

	dialTicket := make(chan uint64, 1)
	c := &cluster{
		dial: func(ctx context.Context, s string, p int) (*conn.conn, error) {
			var id uint64
			select {
			case id = <-dialTicket:
			default:
				return nil, fmt.Errorf("refused")
			}
			cc, err := ln.Dial(ctx)
			ret := conn2.NewConn(cc, conn.connAddr{s, p})
			// Used to distinguish connections.
			ret.Runtime().SetOpStarted(id)
			return ret, err
		},
		balancer: balancer,
		testHookTrackerQueue: func(q []*list.Element) {
			ch := make(chan int)
			tracking <- ch
			ch <- len(q)
		},
	}
	defer func() {
		err := c.Close()
		if err != nil {
			t.Errorf("close failed: %v", err)
		}
	}()

	t.Run("test actual block of tracker", func(t *testing.T) {
		endpoint := endpoint.Endpoint{Addr: "foo"}
		c.Insert(ctx, endpoint)

		// Wait for connection become tracked.
		<-timer.Reset
		assertTracking(1)
		<-timer.Reset

		// Now force tracker to make another iteration, but not release
		// testHookTrackerQueue by reading from tracking channel.
		timer.C <- timeutil.Now()
		blocked := <-tracking

		// While our tracker is in progress (stuck on writing to the tracking
		// channel actually) remove endpoint.
		c.Remove(ctx, endpoint)

		// Now insert back the same endpoint with alive connection (and let dialer
		// to dial successfully).
		dialTicket <- 100
		c.Insert(ctx, endpoint)

		// Release the tracker iteration.
		dialTicket <- 200
		<-blocked
		<-timer.Reset
		assertTracking(0)

		var ss []stats.Stats
		c.Stats(func(_ endpoint.Endpoint, s stats.Stats) {
			ss = append(ss, s)
		})
		if len(ss) != 1 {
			t.Fatalf("unexpected number of connection stats")
		}
		if ss[0].OpStarted != 100 {
			t.Fatalf("unexpected connection used")
		}
	})
}

func TestClusterAwait(t *testing.T) {
	const timeout = 100 * time.Millisecond

	ln := newStubListener()
	srv := grpc.NewServer()
	go func() {
		_ = srv.Serve(ln)
	}()

	var connToReturn *conn.conn
	c := &cluster{
		dial: func(ctx context.Context, _ string, _ int) (_ *conn.conn, err error) {
			cc, err := ln.Dial(ctx)
			if err != nil {
				return nil, err
			}
			return &conn.conn{
				raw: cc,
			}, nil
		},
		balancer: stubBalancer{
			OnInsert: func(c *conn.conn, _ info.Info) cluster2.balancerElement {
				connToReturn = c
				return c.addr
			},
			OnNext: func() *conn.conn {
				return connToReturn
			},
		},
	}
	get := func() (<-chan error, context.CancelFunc) {
		ctx, cancel := context.WithCancel(context.Background())
		got := make(chan error)
		go func() {
			_, err := c.Get(ctx)
			got <- err
		}()
		return got, cancel
	}
	{
		got, cancel := get()
		cancel()
		assertRecvError(t, timeout, got, context.Canceled)
	}
	{
		got, cancel := get()
		defer cancel()
		assertRecvError(t, timeout, got, ErrClusterEmpty)
	}
	{
		c.Insert(context.Background(), endpoint.Endpoint{})
		got, cancel := get()
		defer cancel()
		assertRecvError(t, timeout, got, nil)
	}
}

type stubBalancer struct {
	OnNext      func() *conn.conn
	OnInsert    func(*conn.conn, info.Info) cluster2.balancerElement
	OnUpdate    func(cluster2.balancerElement, info.Info)
	OnRemove    func(cluster2.balancerElement)
	OnPessimize func(cluster2.balancerElement) error
	OnContains  func(cluster2.balancerElement) bool
}

func simpleBalancer() (*list2.List, cluster2.balancer) {
	cs := new(list2.List)
	var i int
	return cs, stubBalancer{
		OnNext: func() *conn.conn {
			n := len(*cs)
			if n == 0 {
				return nil
			}
			e := (*cs)[i%n]
			i++
			return e.conn
		},
		OnInsert: func(conn *conn.conn, info info.Info) cluster2.balancerElement {
			return cs.Insert(conn, info)
		},
		OnRemove: func(x cluster2.balancerElement) {
			e := x.(*list2.Element)
			cs.Remove(e)
		},
		OnUpdate: func(x cluster2.balancerElement, info info.Info) {
			e := x.(*list2.Element)
			e.info = info
		},
		OnPessimize: func(x cluster2.balancerElement) error {
			e := x.(*list2.Element)
			e.conn.runtime.setState(state.Banned)
			return nil
		},
		OnContains: func(x cluster2.balancerElement) bool {
			e := x.(*list2.Element)
			return cs.Contains(e)
		},
	}
}

func (s stubBalancer) Next() *conn.conn {
	if f := s.OnNext; f != nil {
		return f()
	}
	return nil
}
func (s stubBalancer) Insert(c *conn.conn, i info.Info) cluster2.balancerElement {
	if f := s.OnInsert; f != nil {
		return f(c, i)
	}
	return nil
}
func (s stubBalancer) Update(el cluster2.balancerElement, i info.Info) {
	if f := s.OnUpdate; f != nil {
		f(el, i)
	}
}
func (s stubBalancer) Remove(el cluster2.balancerElement) {
	if f := s.OnRemove; f != nil {
		f(el)
	}
}
func (s stubBalancer) Pessimize(el cluster2.balancerElement) error {
	if f := s.OnPessimize; f != nil {
		return f(el)
	}
	return nil
}

func (s stubBalancer) Contains(el cluster2.balancerElement) bool {
	if f := s.OnContains; f != nil {
		return f(el)
	}
	return false
}

type stubListener struct {
	C chan net.Conn // Client half of the connection.
	S chan net.Conn // Server half of the connection.

	once sync.Once
	exit chan struct{}
}

func newStubListener() *stubListener {
	return &stubListener{
		C: make(chan net.Conn),
		S: make(chan net.Conn, 1),

		exit: make(chan struct{}),
	}
}

func (ln *stubListener) Accept() (net.Conn, error) {
	s, c := net.Pipe()
	select {
	case ln.C <- c:
	case <-ln.exit:
		return nil, fmt.Errorf("closed")
	}
	select {
	case ln.S <- s:
	default:
	}
	return s, nil
}

func (ln *stubListener) Addr() net.Addr {
	return &net.TCPAddr{}
}

func (ln *stubListener) Close() error {
	ln.once.Do(func() {
		close(ln.exit)
	})
	return nil
}

func (ln *stubListener) Dial(ctx context.Context) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, "",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			select {
			case <-ln.exit:
				return nil, fmt.Errorf("refused")
			case c := <-ln.C:
				return c, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}),
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    time.Second,
			Timeout: time.Second,
		}),
	)
}

func assertRecvError(t *testing.T, d time.Duration, e <-chan error, exp error) {
	select {
	case act := <-e:
		if act != exp {
			t.Errorf("%s: unexpected error: %v; want %v", ydb.fileLine(2), act, exp)
		}
	case <-time.After(d):
		t.Errorf("%s: nothing received after %s", ydb.fileLine(2), d)
	}
}

func mergeEndpointIntoCluster(ctx context.Context, c *cluster, curr, next []endpoint.Endpoint) {
	SortEndpoints(curr)
	SortEndpoints(next)
	DiffEndpoints(curr, next,
		func(i, j int) { c.Update(ctx, next[j]) },
		func(i, j int) { c.Insert(ctx, next[j]) },
		func(i, j int) { c.Remove(ctx, curr[i]) },
	)
}

func TestDiffEndpoint(t *testing.T) {
	// lists must be sorted
	noEndpoints := []endpoint.Endpoint{}
	someEndpoints := []endpoint.Endpoint{
		{
			Addr: "0",
			Port: 0,
		},
		{
			Addr: "1",
			Port: 1,
		},
	}
	sameSomeEndpoints := []endpoint.Endpoint{
		{
			Addr:       "0",
			Port:       0,
			LoadFactor: 1,
			Local:      true,
		},
		{
			Addr:       "1",
			Port:       1,
			LoadFactor: 2,
			Local:      true,
		},
	}
	anotherEndpoints := []endpoint.Endpoint{
		{
			Addr: "2",
			Port: 0,
		},
		{
			Addr: "3",
			Port: 1,
		},
	}
	moreEndpointsOverlap := []endpoint.Endpoint{
		{
			Addr:       "0",
			Port:       0,
			LoadFactor: 1,
			Local:      true,
		},
		{
			Addr: "1",
			Port: 1,
		},
		{
			Addr: "1",
			Port: 2,
		},
	}

	type TC struct {
		name         string
		curr, next   []endpoint.Endpoint
		eq, add, del int
	}

	tests := []TC{
		{
			name: "none",
			curr: noEndpoints,
			next: noEndpoints,
			eq:   0,
			add:  0,
			del:  0,
		},
		{
			name: "equals",
			curr: someEndpoints,
			next: sameSomeEndpoints,
			eq:   2,
			add:  0,
			del:  0,
		},
		{
			name: "noneToSome",
			curr: noEndpoints,
			next: someEndpoints,
			eq:   0,
			add:  2,
			del:  0,
		},
		{
			name: "SomeToNone",
			curr: someEndpoints,
			next: noEndpoints,
			eq:   0,
			add:  0,
			del:  2,
		},
		{
			name: "SomeToMore",
			curr: someEndpoints,
			next: moreEndpointsOverlap,
			eq:   2,
			add:  1,
			del:  0,
		},
		{
			name: "MoreToSome",
			curr: moreEndpointsOverlap,
			next: someEndpoints,
			eq:   2,
			add:  0,
			del:  1,
		},
		{
			name: "SomeToAnother",
			curr: someEndpoints,
			next: anotherEndpoints,
			eq:   0,
			add:  2,
			del:  2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			eq, add, del := 0, 0, 0
			DiffEndpoints(tc.curr, tc.next,
				func(i, j int) { eq++ },
				func(i, j int) { add++ },
				func(i, j int) { del++ },
			)
			if eq != tc.eq || add != tc.add || del != tc.del {
				t.Errorf("Got %d, %d, %d expected: %d, %d, %d", eq, add, del, tc.eq, tc.add, tc.del)
			}
		})
	}
}
