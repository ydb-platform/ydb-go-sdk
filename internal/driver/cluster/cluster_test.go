package cluster

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/entry"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/info"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/list"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/runtime/stats/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/stub"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/endpoint"
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
		index:    make(map[endpoint.Addr]entry.Entry),
	}

	pingConnects := func(size int) chan struct{} {
		done := make(chan struct{})
		go func() {
			for i := 0; i < size*10; i++ {
				conn, err := c.Get(context.Background())
				// enforce close bad connects to track them
				if err == nil && conn != nil && conn.Addr().Host == "bad" {
					err = conn.Close()
					if err == nil {
						break
					}
				}
			}
			close(done)
		}()
		return done
	}

	ne := []endpoint.Endpoint{
		{Addr: endpoint.Addr{Host: "foo"}},
		{Addr: endpoint.Addr{Host: "bad"}},
	}
	mergeEndpointIntoCluster(ctx, c, []endpoint.Endpoint{}, ne, WithConnConfig(stub.Config(config.New())))
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
		index:    make(map[endpoint.Addr]entry.Entry),
	}

	assert := func(t *testing.T, exp []endpoint.Endpoint) {
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

	endpoints := []endpoint.Endpoint{
		{Addr: endpoint.Addr{Host: "foo"}},
		{Addr: endpoint.Addr{Host: "foo", Port: 123}},
	}
	badEndpoints := []endpoint.Endpoint{
		{Addr: endpoint.Addr{Host: "baz"}},
		{Addr: endpoint.Addr{Host: "baz", Port: 123}},
	}
	nextEndpoints := []endpoint.Endpoint{
		{Addr: endpoint.Addr{Host: "foo"}},
		{Addr: endpoint.Addr{Host: "bar"}},
		{Addr: endpoint.Addr{Host: "bar", Port: 123}},
	}
	nextBadEndpoints := []endpoint.Endpoint{
		{Addr: endpoint.Addr{Host: "bad", Port: 23}},
	}
	t.Run("initial fill", func(t *testing.T) {
		ne := append(endpoints, badEndpoints...)
		// merge new endpoints into balancer
		mergeEndpointIntoCluster(ctx, c, []endpoint.Endpoint{}, ne, WithConnConfig(stub.Config(config.New())))
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

func mergeEndpointIntoCluster(ctx context.Context, c *cluster, curr, next []endpoint.Endpoint, opts ...option) {
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
	noEndpoints := []endpoint.Endpoint{}
	someEndpoints := []endpoint.Endpoint{
		{
			Addr: endpoint.Addr{
				Host: "0",
				Port: 0,
			},
		},
		{
			Addr: endpoint.Addr{
				Host: "1",
				Port: 1,
			},
		},
	}
	sameSomeEndpoints := []endpoint.Endpoint{
		{
			Addr: endpoint.Addr{
				Host: "0",
				Port: 0,
			},
			LoadFactor: 1,
			Local:      true,
		},
		{
			Addr: endpoint.Addr{
				Host: "1",
				Port: 1,
			},
			LoadFactor: 2,
			Local:      true,
		},
	}
	anotherEndpoints := []endpoint.Endpoint{
		{
			Addr: endpoint.Addr{
				Host: "2",
				Port: 0,
			},
		},
		{
			Addr: endpoint.Addr{
				Host: "3",
				Port: 1,
			},
		},
	}
	moreEndpointsOverlap := []endpoint.Endpoint{
		{
			Addr: endpoint.Addr{
				Host: "0",
				Port: 0,
			},
			LoadFactor: 1,
			Local:      true,
		},
		{
			Addr: endpoint.Addr{
				Host: "1",
				Port: 1,
			},
		},
		{
			Addr: endpoint.Addr{
				Host: "1",
				Port: 2,
			},
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
