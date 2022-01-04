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
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/ibalancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/stub"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/cluster/entry"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	connConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
)

func TestClusterFastRedial(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	listener := newStubListener()
	server := grpc.NewServer()
	go func() {
		_ = server.Serve(listener)
	}()

	l, b := stub.Balancer()
	c := &cluster{
		dial: func(ctx context.Context, address string) (*grpc.ClientConn, error) {
			return listener.Dial(ctx)
		},
		balancer:  b,
		index:     make(map[string]entry.Entry),
		endpoints: make(map[uint32]conn.Conn),
	}

	pingConnects := func(size int) chan struct{} {
		done := make(chan struct{})
		go func() {
			for i := 0; i < size*10; i++ {
				c, err := c.Get(context.Background())
				// enforce close bad connects to track them
				if err == nil && c != nil && c.Endpoint().Address() == "bad:0" {
					_ = c.Close(ctx)
				}
			}
			close(done)
		}()
		return done
	}

	ne := []endpoint.Endpoint{
		endpoint.New("foo:0"),
		endpoint.New("bad:0"),
	}
	mergeEndpointIntoCluster(ctx, c, []endpoint.Endpoint{}, ne, WithConnConfig(connConfig.Config(config.New())))
	select {
	case <-pingConnects(len(ne)):

	case <-time.After(time.Second * 15):
		t.Fatalf("Time limit exceeded while %d endpoints in balance. Wait channel used", len(*l))
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

	c := &cluster{
		dial: func(ctx context.Context, address string) (*grpc.ClientConn, error) {
			return ln.Dial(ctx)
		},
		balancer: func() ibalancer.Balancer {
			_, b := stub.Balancer()
			return b
		}(),
		index:     make(map[string]entry.Entry),
		endpoints: make(map[uint32]conn.Conn),
	}

	assert := func(t *testing.T, exp []endpoint.Endpoint) {
		if len(c.index) != len(exp) {
			t.Fatalf("unexpected number of endpoints %d: got %d", len(exp), len(c.index))
		}
		for _, e := range exp {
			if _, ok := c.index[e.Address()]; !ok {
				t.Fatalf("not found endpoint '%v' in index", e.Address())
			}
		}
		for _, entry := range c.index {
			if func() bool {
				for _, e := range exp {
					if e.Address() == entry.Conn.Endpoint().Address() {
						return false
					}
				}
				return true
			}() {
				t.Fatalf("unexpected endpoint '%v' in index", entry.Conn.Endpoint().Address())
			}
		}
	}

	endpoints := []endpoint.Endpoint{
		endpoint.New("foo:0"),
		endpoint.New("foo:123"),
	}
	badEndpoints := []endpoint.Endpoint{
		endpoint.New("baz:0"),
		endpoint.New("baz:123"),
	}
	nextEndpoints := []endpoint.Endpoint{
		endpoint.New("foo:0"),
		endpoint.New("bar:0"),
		endpoint.New("bar:123"),
	}
	nextBadEndpoints := []endpoint.Endpoint{
		endpoint.New("bad:23"),
	}
	t.Run("initial fill", func(t *testing.T) {
		// nolint: gocritic
		// nolint: nolintlint
		ne := append(endpoints, badEndpoints...)
		// merge new endpoints into balancer
		mergeEndpointIntoCluster(
			ctx,
			c,
			[]endpoint.Endpoint{},
			ne,
			WithConnConfig(connConfig.Config(config.New())),
		)
		// try endpoints, filter out bad ones to tracking
		assert(t, ne)
	})
	t.Run("update with another endpoints", func(t *testing.T) {
		// nolint: gocritic
		// nolint: nolintlint
		ne := append(nextEndpoints, nextBadEndpoints...)
		// merge new endpoints into balancer
		mergeEndpointIntoCluster(
			ctx,
			c,
			append(endpoints, badEndpoints...),
			ne,
			WithConnConfig(connConfig.Config(config.New())),
		)
		// try endpoints, filter out bad ones to tracking
		assert(t, ne)
	})
	t.Run("left only bad", func(t *testing.T) {
		ne := nextBadEndpoints
		// merge new endpoints into balancer
		mergeEndpointIntoCluster(
			ctx,
			c,
			append(nextEndpoints, nextBadEndpoints...),
			ne,
		)
		// try endpoints, filter out bad ones to tracking
		assert(t, ne)
	})
	t.Run("left only good", func(t *testing.T) {
		ne := nextEndpoints
		// merge new endpoints into balancer
		mergeEndpointIntoCluster(
			ctx,
			c,
			nextBadEndpoints,
			ne,
			WithConnConfig(connConfig.Config(config.New())),
		)
		// try endpoints, filter out bad ones to tracking
		assert(t, ne)
	})
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
	var noEndpoints []endpoint.Endpoint
	someEndpoints := []endpoint.Endpoint{
		endpoint.New("0:0"),
		endpoint.New("1:1"),
	}
	sameSomeEndpoints := []endpoint.Endpoint{
		endpoint.New("0:0", endpoint.WithLoadFactor(1), endpoint.WithLocalDC(true)),
		endpoint.New("1:1", endpoint.WithLoadFactor(2), endpoint.WithLocalDC(true)),
	}
	anotherEndpoints := []endpoint.Endpoint{
		endpoint.New("2:0"),
		endpoint.New("3:1"),
	}
	moreEndpointsOverlap := []endpoint.Endpoint{
		endpoint.New("0:0", endpoint.WithLoadFactor(1), endpoint.WithLocalDC(true)),
		endpoint.New("1:1"),
		endpoint.New("1:2"),
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
