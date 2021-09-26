package cluster

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	public "github.com/ydb-platform/ydb-go-sdk/v3/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/entry"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/info"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/list"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/runtime/stats"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/runtime/stats/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/stub"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil/timeutil"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil/timeutil/timetest"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
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
		dial: func(ctx context.Context, s string, p int) (*grpc.ClientConn, error) {
			return ln.Dial(ctx)
		},
		balancer: balancer,
		index:    make(map[public.Addr]entry.Entry),
	}

	pingConnects := func(size int) chan struct{} {
		done := make(chan struct{})
		go func() {
			for i := 0; i < size*10; i++ {
				conn, err := c.Get(context.Background())
				// enforce close bad connects to track them
				if err == nil && conn != nil && conn.Addr().Host == "bad" {
					_ = conn.Close()
				}
			}
			close(done)
		}()
		return done
	}

	ne := []public.Endpoint{
		{Addr: public.Addr{Host: "foo"}},
		{Addr: public.Addr{Host: "bad"}},
	}
	mergeEndpointIntoCluster(ctx, c, []public.Endpoint{}, ne, WithConnConfig(stub.Config(config.New())))
	select {
	case <-pingConnects(len(ne)):

	case <-time.After(time.Second * 10):
		t.Fatalf("Time limit exceeded while %d endpoints in balance. Wait channel used", len(*cs))
	}
}

func TestClusterMergeEndpoints(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ln := newStubListener()
	srv := grpc.NewServer()
	go func() {
		_ = srv.Serve(ln)
	}()

	_, balancer := simpleBalancer()
	c := &cluster{
		dial: func(ctx context.Context, s string, p int) (*grpc.ClientConn, error) {
			return ln.Dial(ctx)
		},
		balancer: balancer,
		index:    make(map[public.Addr]entry.Entry),
	}

	assert := func(t *testing.T, exp []public.Endpoint) {
		if len(c.index) != len(exp) {
			t.Fatalf("unexpected number of endpoints %d: got %d", len(exp), len(c.index))
		}
		for _, e := range exp {
			if _, ok := c.index[e.Addr]; !ok {
				t.Fatalf("not found endpoint '%v' in index", e.String())
			}
		}
		for addr := range c.index {
			if func() bool {
				for _, e := range exp {
					if e.Addr == addr {
						return false
					}
				}
				return true
			}() {
				t.Fatalf("unexpected endpoint '%v' in index", addr.String())
			}
		}
	}

	endpoints := []public.Endpoint{
		{Addr: public.Addr{Host: "foo"}},
		{Addr: public.Addr{Host: "foo", Port: 123}},
	}
	badEndpoints := []public.Endpoint{
		{Addr: public.Addr{Host: "baz"}},
		{Addr: public.Addr{Host: "baz", Port: 123}},
	}
	nextEndpoints := []public.Endpoint{
		{Addr: public.Addr{Host: "foo"}},
		{Addr: public.Addr{Host: "bar"}},
		{Addr: public.Addr{Host: "bar", Port: 123}},
	}
	nextBadEndpoints := []public.Endpoint{
		{Addr: public.Addr{Host: "bad", Port: 23}},
	}
	t.Run("initial fill", func(t *testing.T) {
		ne := append(endpoints, badEndpoints...)
		// merge new endpoints into balancer
		mergeEndpointIntoCluster(ctx, c, []public.Endpoint{}, ne, WithConnConfig(stub.Config(config.New())))
		// try endpoints, filter out bad ones to tracking
		assert(t, ne)
	})
	t.Run("update with another endpoints", func(t *testing.T) {
		ne := append(nextEndpoints, nextBadEndpoints...)
		// merge new endpoints into balancer
		mergeEndpointIntoCluster(ctx, c, append(endpoints, badEndpoints...), ne, WithConnConfig(stub.Config(config.New())))
		// try endpoints, filter out bad ones to tracking
		assert(t, ne)
	})
	t.Run("left only bad", func(t *testing.T) {
		ne := nextBadEndpoints
		// merge new endpoints into balancer
		mergeEndpointIntoCluster(ctx, c, append(nextEndpoints, nextBadEndpoints...), ne)
		// try endpoints, filter out bad ones to tracking
		assert(t, ne)
	})
	t.Run("left only good", func(t *testing.T) {
		ne := nextEndpoints
		// merge new endpoints into balancer
		mergeEndpointIntoCluster(ctx, c, nextBadEndpoints, ne, WithConnConfig(stub.Config(config.New())))
		// try endpoints, filter out bad ones to tracking
		assert(t, ne)
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
		dial: func(ctx context.Context, s string, p int) (*grpc.ClientConn, error) {
			return ln.Dial(ctx)
		},
		balancer: balancer,
		index:    make(map[public.Addr]entry.Entry),
	}

	endpoint := public.Endpoint{Addr: public.Addr{Host: "foo"}}
	c.Insert(ctx, endpoint, WithConnConfig(stub.Config(config.New())))

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

	c.Remove(ctx, endpoint, WithConnConfig(stub.Config(config.New())))

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
		dial: func(ctx context.Context, s string, p int) (*grpc.ClientConn, error) {
			return nil, fmt.Errorf("refused")
		},
		balancer: balancer,
	}

	endpoint := public.Endpoint{Addr: public.Addr{Host: "foo"}}
	c.Insert(ctx, endpoint, WithConnConfig(stub.Config(config.New())))
	<-timer.Reset

	c.Remove(ctx, endpoint, WithConnConfig(stub.Config(config.New())))

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
		dial: func(ctx context.Context, s string, p int) (*grpc.ClientConn, error) {
			return ln.Dial(ctx)
		},
		balancer: balancer,
		index:    make(map[public.Addr]entry.Entry),
	}
	defer func() {
		err := c.Close()
		if err != nil {
			t.Errorf("close failed: %v", err)
		}
	}()

	t.Run("test actual block of tracker", func(t *testing.T) {
		endpoint := public.Endpoint{
			Addr: public.Addr{
				Host: "foo",
			},
		}
		c.Insert(ctx, endpoint, WithConnConfig(stub.Config(config.New())))

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
		c.Remove(ctx, endpoint, WithConnConfig(stub.Config(config.New())))

		// Now insert back the same endpoint with alive connection (and let dialer
		// to dial successfully).
		dialTicket <- 100
		c.Insert(ctx, endpoint, WithConnConfig(stub.Config(config.New())))

		// Release the tracker iteration.
		dialTicket <- 200
		<-blocked
		<-timer.Reset
		assertTracking(0)

		var ss []stats.Stats
		c.Stats(func(_ public.Endpoint, s stats.Stats) {
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

	var connToReturn conn.Conn
	c := &cluster{
		dial: func(ctx context.Context, _ string, _ int) (_ *grpc.ClientConn, err error) {
			return ln.Dial(ctx)
		},
		balancer: stubBalancer{
			OnInsert: func(c conn.Conn, _ info.Info) balancer.Element {
				connToReturn = c
				return c.Addr()
			},
			OnNext: func() conn.Conn {
				return connToReturn
			},
		},
		index: make(map[public.Addr]entry.Entry),
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
		c.Insert(context.Background(), public.Endpoint{}, WithConnConfig(stub.Config(config.New())))
		got, cancel := get()
		defer cancel()
		assertRecvError(t, timeout, got, nil)
	}
}

type stubBalancer struct {
	OnNext      func() conn.Conn
	OnInsert    func(conn.Conn, info.Info) balancer.Element
	OnUpdate    func(balancer.Element, info.Info)
	OnRemove    func(balancer.Element)
	OnPessimize func(balancer.Element) error
	OnContains  func(balancer.Element) bool
}

func simpleBalancer() (*list.List, balancer.Balancer) {
	cs := new(list.List)
	var i int
	return cs, stubBalancer{
		OnNext: func() conn.Conn {
			n := len(*cs)
			if n == 0 {
				return nil
			}
			e := (*cs)[i%n]
			i++
			return e.Conn
		},
		OnInsert: func(conn conn.Conn, info info.Info) balancer.Element {
			return cs.Insert(conn, info)
		},
		OnRemove: func(x balancer.Element) {
			e := x.(*list.Element)
			cs.Remove(e)
		},
		OnUpdate: func(x balancer.Element, info info.Info) {
			e := x.(*list.Element)
			e.Info = info
		},
		OnPessimize: func(x balancer.Element) error {
			e := x.(*list.Element)
			e.Conn.Runtime().SetState(state.Banned)
			return nil
		},
		OnContains: func(x balancer.Element) bool {
			e := x.(*list.Element)
			return cs.Contains(e)
		},
	}
}

func (s stubBalancer) Next() conn.Conn {
	if f := s.OnNext; f != nil {
		return f()
	}
	return nil
}
func (s stubBalancer) Insert(c conn.Conn, i info.Info) balancer.Element {
	if f := s.OnInsert; f != nil {
		return f(c, i)
	}
	return nil
}
func (s stubBalancer) Update(el balancer.Element, i info.Info) {
	if f := s.OnUpdate; f != nil {
		f(el, i)
	}
}
func (s stubBalancer) Remove(el balancer.Element) {
	if f := s.OnRemove; f != nil {
		f(el)
	}
}
func (s stubBalancer) Pessimize(el balancer.Element) error {
	if f := s.OnPessimize; f != nil {
		return f(el)
	}
	return nil
}

func (s stubBalancer) Contains(el balancer.Element) bool {
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
			t.Errorf("%s: unexpected error: %v; want %v", testutil.FileLine(2), act, exp)
		}
	case <-time.After(d):
		t.Errorf("%s: nothing received after %s", testutil.FileLine(2), d)
	}
}

func mergeEndpointIntoCluster(ctx context.Context, c *cluster, curr, next []public.Endpoint, opts ...option) {
	SortEndpoints(curr)
	SortEndpoints(next)
	DiffEndpoints(curr, next,
		func(i, j int) {
			c.Update(ctx, next[j], opts...)
		},
		func(i, j int) {
			c.Insert(ctx, next[j], opts...)
		},
		func(i, j int) {
			c.Remove(ctx, curr[i], opts...)
		},
	)
}

func TestDiffEndpoint(t *testing.T) {
	// lists must be sorted
	noEndpoints := []public.Endpoint{}
	someEndpoints := []public.Endpoint{
		{
			Addr: public.Addr{
				Host: "0",
				Port: 0,
			},
		},
		{
			Addr: public.Addr{
				Host: "1",
				Port: 1,
			},
		},
	}
	sameSomeEndpoints := []public.Endpoint{
		{
			Addr: public.Addr{
				Host: "0",
				Port: 0,
			},
			LoadFactor: 1,
			Local:      true,
		},
		{
			Addr: public.Addr{
				Host: "1",
				Port: 1,
			},
			LoadFactor: 2,
			Local:      true,
		},
	}
	anotherEndpoints := []public.Endpoint{
		{
			Addr: public.Addr{
				Host: "2",
				Port: 0,
			},
		},
		{
			Addr: public.Addr{
				Host: "3",
				Port: 1,
			},
		},
	}
	moreEndpointsOverlap := []public.Endpoint{
		{
			Addr: public.Addr{
				Host: "0",
				Port: 0,
			},
			LoadFactor: 1,
			Local:      true,
		},
		{
			Addr: public.Addr{
				Host: "1",
				Port: 1,
			},
		},
		{
			Addr: public.Addr{
				Host: "1",
				Port: 2,
			},
		},
	}

	type TC struct {
		name         string
		curr, next   []public.Endpoint
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
