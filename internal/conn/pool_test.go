package conn

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// mockConfig implements the Config interface for testing
type mockConfig struct {
	dialTimeout  time.Duration
	driverTrace  *trace.Driver
	grpcDialOpts []grpc.DialOption
}

func (m *mockConfig) DialTimeout() time.Duration {
	return m.dialTimeout
}

func (m *mockConfig) Trace() *trace.Driver {
	if m.driverTrace == nil {
		return &trace.Driver{}
	}

	return m.driverTrace
}

func (m *mockConfig) GrpcDialOptions() []grpc.DialOption {
	return m.grpcDialOpts
}

func testPoolUsages(p *Pool) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.usages
}

func testPoolConnsLen(p *Pool) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return len(p.conns)
}

func testPoolHasConn(p *Pool, key endpoint.Key) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	_, ok := p.conns[key]

	return ok
}

func testPoolConnValue(p *Pool, key endpoint.Key) (*connValue, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	v, ok := p.conns[key]

	return v, ok
}

func endpointsToConnections(p *Pool, endpoints []endpoint.Endpoint) []Conn {
	p.mu.Lock()
	defer p.mu.Unlock()

	conns := make([]Conn, 0, len(endpoints))
	for _, e := range endpoints {
		cv := p.get(e)
		if cv != nil {
			conns = append(conns, cv.cc)
		}
	}

	return conns
}

func testPoolConnUseCount(p *Pool, key endpoint.Key) (int64, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	value, ok := p.conns[key]
	if !ok || value == nil {
		return 0, false
	}

	return value.useCount, true
}

func TestPool_Get(t *testing.T) {
	t.Run("GetSameConnectionTwice", func(t *testing.T) {
		ctx := context.Background()
		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		pool := NewPool(ctx, config)
		defer func() {
			_ = pool.RemoveRef(ctx)
		}()

		e := endpoint.New("test-endpoint:2135")

		conn1 := pool.Get(e)
		require.NotNil(t, conn1)

		conn2 := pool.Get(e)
		require.NotNil(t, conn2)

		// Should return the same connection
		require.Equal(t, conn1, conn2)
	})

	t.Run("GetDifferentEndpoints", func(t *testing.T) {
		ctx := context.Background()
		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		pool := NewPool(ctx, config)
		defer func() {
			_ = pool.RemoveRef(ctx)
		}()

		e1 := endpoint.New("endpoint1:2135")
		e2 := endpoint.New("endpoint2:2135")

		conn1 := pool.Get(e1)
		require.NotNil(t, conn1)

		conn2 := pool.Get(e2)
		require.NotNil(t, conn2)

		// Should return different connections
		require.NotEqual(t, conn1, conn2)
	})
}

func TestPool_TakeRelease(t *testing.T) {
	t.Run("TakeIncreasesUsageCounter", func(t *testing.T) {
		ctx := context.Background()
		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		pool := NewPool(ctx, config)

		// Initial usage is 1
		require.Equal(t, int64(1), testPoolUsages(pool))

		err := pool.AddRef(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(2), testPoolUsages(pool))

		err = pool.AddRef(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(3), testPoolUsages(pool))

		// Clean up
		_ = pool.RemoveRef(ctx)
		_ = pool.RemoveRef(ctx)
		_ = pool.RemoveRef(ctx)
	})

	t.Run("ReleaseDecreasesUsageCounter", func(t *testing.T) {
		ctx := context.Background()
		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		pool := NewPool(ctx, config)

		err := pool.AddRef(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(2), testPoolUsages(pool))

		err = pool.RemoveRef(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(1), testPoolUsages(pool))

		// Clean up
		_ = pool.RemoveRef(ctx)
	})

	t.Run("FinalReleaseClosesPool", func(t *testing.T) {
		ctx := context.Background()
		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		pool := NewPool(ctx, config)

		// Get a connection to ensure the pool has something to close
		e := endpoint.New("test-endpoint:2135")
		conn := pool.Get(e)
		require.NotNil(t, conn)

		// Final release should close the pool
		err := pool.RemoveRef(ctx)
		require.NoError(t, err)

		// Pool should be closed
		require.True(t, pool.closed.Load())
	})
}

func TestPool_IsClosed(t *testing.T) {
	t.Run("NewPoolIsNotClosed", func(t *testing.T) {
		ctx := context.Background()
		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		pool := NewPool(ctx, config)
		defer func() {
			_ = pool.RemoveRef(ctx)
		}()

		require.False(t, pool.closed.Load())
	})

	t.Run("ReleasedPoolIsClosed", func(t *testing.T) {
		ctx := context.Background()
		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		pool := NewPool(ctx, config)

		err := pool.RemoveRef(ctx)
		require.NoError(t, err)

		require.True(t, pool.closed.Load())
	})
}

func TestPool_ConfigMethods(t *testing.T) {
	t.Run("DialTimeout", func(t *testing.T) {
		ctx := context.Background()
		config := &mockConfig{
			dialTimeout: 10 * time.Second,
		}
		pool := NewPool(ctx, config)
		defer func() {
			_ = pool.RemoveRef(ctx)
		}()

		require.Equal(t, 10*time.Second, pool.DialTimeout())
	})

	t.Run("Trace", func(t *testing.T) {
		ctx := context.Background()
		driverTrace := &trace.Driver{}
		config := &mockConfig{
			dialTimeout: 5 * time.Second,
			driverTrace: driverTrace,
		}
		pool := NewPool(ctx, config)
		defer func() {
			_ = pool.RemoveRef(ctx)
		}()

		require.Equal(t, driverTrace, pool.Trace())
	})

	t.Run("GrpcDialOptions", func(t *testing.T) {
		ctx := context.Background()
		opts := []grpc.DialOption{grpc.WithBlock()} //nolint:staticcheck,nolintlint
		config := &mockConfig{
			dialTimeout:  5 * time.Second,
			grpcDialOpts: opts,
		}
		pool := NewPool(ctx, config)
		defer func() {
			_ = pool.RemoveRef(ctx)
		}()

		// The pool adds additional options, so we check the length is at least the ones we provided
		require.GreaterOrEqual(t, len(pool.GrpcDialOptions()), len(opts))
	})
}

func TestEndpointsToConnections(t *testing.T) {
	t.Run("CreatesConnectionsForEndpoints", func(t *testing.T) {
		ctx := context.Background()
		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		pool := NewPool(ctx, config)
		defer func() {
			_ = pool.RemoveRef(ctx)
		}()

		require.Equal(t, 0, testPoolConnsLen(pool))

		e1 := endpoint.New("e1:2135")
		e2 := endpoint.New("e2:2135")

		conns := endpointsToConnections(pool, []endpoint.Endpoint{e1, e2})

		require.Len(t, conns, 2)
		require.Equal(t, 2, testPoolConnsLen(pool))

		require.Equal(t, pool.Get(e1), conns[0])
		require.Equal(t, pool.Get(e2), conns[1])
	})

	t.Run("ReusesExistingConnections", func(t *testing.T) {
		ctx := context.Background()
		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		pool := NewPool(ctx, config)
		defer func() {
			_ = pool.RemoveRef(ctx)
		}()

		e := endpoint.New("reuse:2135")

		existing := pool.Get(e)
		require.NotNil(t, existing)

		initialLen := testPoolConnsLen(pool)

		conns := endpointsToConnections(pool, []endpoint.Endpoint{e})

		require.Len(t, conns, 1)
		require.Equal(t, existing, conns[0])

		require.Equal(t, initialLen, testPoolConnsLen(pool))
	})

	t.Run("IPv6AndHostOverrideUniqueKeys", func(t *testing.T) {
		ctx := context.Background()
		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		pool := NewPool(ctx, config)
		defer func() {
			_ = pool.RemoveRef(ctx)
		}()

		// ensure empty pool
		require.Equal(t, 0, testPoolConnsLen(pool))

		// address is a dns-style host:port, ipv6 provides resolved ip used in Key.Address()
		e1 := endpoint.New("example.com:2135", endpoint.WithIPV6([]string{"2001:db8::1"}))
		// if node is rebooted with different ssl name override, we need a different connection
		e2 := endpoint.New(
			"example.com:2135",
			endpoint.WithIPV6([]string{"2001:db8::1"}),
			endpoint.WithSslTargetNameOverride("override"),
		)
		// different ipv6 -> different Address()
		e3 := endpoint.New("example.com:2135", endpoint.WithIPV6([]string{"2001:db8::2"}), endpoint.WithID(2))
		e4 := endpoint.New("example.com:2135", endpoint.WithIPV6([]string{"2001:db8::3"}), endpoint.WithID(2))
		// same ipv6 as e1 but different NodeID -> different Key.NodeID
		e5 := endpoint.New("example.com:2135", endpoint.WithIPV6([]string{"2001:db8::1"}), endpoint.WithID(1))

		endpoints := []endpoint.Endpoint{e1, e2, e3, e4, e5}
		conns := endpointsToConnections(pool, endpoints)

		require.Len(t, conns, len(endpoints))
		require.Equal(t, 5, testPoolConnsLen(pool))

		for i, e := range endpoints {
			got := conns[i]
			require.NotNil(t, got)
			require.Equal(t, pool.Get(e), got)
			cc, ok := testPoolConnValue(pool, e.Key())
			require.True(t, ok)
			require.Equal(t, cc.cc, got)
		}

		require.Equal(t, e2.Key().HostOverride, "override")
		require.Equal(t, e4.Key().NodeID, uint32(2))
		require.Equal(t, e5.Key().NodeID, uint32(1))
	})

	t.Run("AddNewEndpointAndNodeIDVariation", func(t *testing.T) {
		ctx := context.Background()
		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		pool := NewPool(ctx, config)
		defer func() {
			_ = pool.RemoveRef(ctx)
		}()

		// initial two endpoints with IPv6 and distinct NodeIDs
		e1 := endpoint.New("e1.example:2135", endpoint.WithIPV6([]string{"2001:db8::1"}), endpoint.WithID(1))
		e2 := endpoint.New("e2.example:2135", endpoint.WithIPV6([]string{"2001:db8::2"}), endpoint.WithID(2))

		// create initial connections
		initialConns := endpointsToConnections(pool, []endpoint.Endpoint{e1, e2})
		require.Len(t, initialConns, 2)
		require.Equal(t, 2, testPoolConnsLen(pool))
		require.Equal(t, pool.Get(e1), initialConns[0])
		require.Equal(t, pool.Get(e2), initialConns[1])

		// add a new unique endpoint e3 -> pool should grow
		e3 := endpoint.New("e3.example:2135", endpoint.WithIPV6([]string{"2001:db8::3"}), endpoint.WithID(3))
		connsAfterE3 := endpointsToConnections(pool, []endpoint.Endpoint{e1, e2, e3})
		require.Len(t, connsAfterE3, 3)
		require.Equal(t, 3, testPoolConnsLen(pool))
		require.Equal(t, pool.Get(e3), connsAfterE3[2])

		// now use same address as e1 but different NodeID (and same ipv6) -> should create new conn
		e1DifferentNode := endpoint.New("e1.example:2135", endpoint.WithIPV6([]string{"2001:db8::1"}), endpoint.WithID(99))
		connsAfterNodeChange := endpointsToConnections(pool, []endpoint.Endpoint{e1DifferentNode})
		require.Len(t, connsAfterNodeChange, 1)
		// pool size must increase by one
		require.Equal(t, 4, testPoolConnsLen(pool))
		// returned conn corresponds to the new endpoint key
		require.Equal(t, pool.Get(e1DifferentNode), connsAfterNodeChange[0])
		require.Equal(t, pool.Get(e1), initialConns[0])
	})
}

func TestPool_GetPut(t *testing.T) {
	t.Run("PutClosesWhenUseCountReachesZero", func(t *testing.T) {
		ctx := context.Background()
		pool := NewPool(ctx, &mockConfig{})
		defer func() {
			_ = pool.RemoveRef(ctx)
		}()

		e := endpoint.New("node:2135")
		c := pool.Get(e)
		require.True(t, testPoolHasConn(pool, e.Key()))

		pool.Put(ctx, c)
		require.False(t, testPoolHasConn(pool, e.Key()))
	})

	t.Run("DropsRemovedEndpointsOnNextDiscovery", func(t *testing.T) {
		ctx := context.Background()
		pool := NewPool(ctx, &mockConfig{})
		defer func() {
			_ = pool.RemoveRef(ctx)
		}()

		e1 := endpoint.New("node1:2135", endpoint.WithID(1))
		e2 := endpoint.New("node2:2135", endpoint.WithID(2))

		c1 := pool.Get(e1)
		c2 := pool.Get(e2)
		require.True(t, testPoolHasConn(pool, e1.Key()))
		require.True(t, testPoolHasConn(pool, e2.Key()))

		quarantine := []Conn{c2}
		for _, c := range quarantine {
			pool.Put(ctx, c)
		}
		require.True(t, testPoolHasConn(pool, e1.Key()))
		require.False(t, testPoolHasConn(pool, e2.Key()))

		pool.Put(ctx, c1)
		require.False(t, testPoolHasConn(pool, e1.Key()))
	})

	t.Run("SharedPoolMultipleBalancers", func(t *testing.T) {
		ctx := context.Background()
		pool := NewPool(ctx, &mockConfig{})
		defer func() {
			_ = pool.RemoveRef(ctx)
		}()

		e := endpoint.New("shared-endpoint:2135")
		balancer1 := pool.Get(e)
		balancer2 := pool.Get(e)
		require.True(t, testPoolHasConn(pool, e.Key()))

		pool.Put(ctx, balancer1)
		require.True(t, testPoolHasConn(pool, e.Key()))

		pool.Put(ctx, balancer2)
		require.False(t, testPoolHasConn(pool, e.Key()))
	})

	t.Run("BootstrapConnSurvivesBalancerRelease", func(t *testing.T) {
		ctx := context.Background()
		pool := NewPool(ctx, &mockConfig{})
		defer func() {
			_ = pool.RemoveRef(ctx)
		}()

		e := endpoint.New("bootstrap:2135")
		bootstrap := pool.Get(e)
		balancer := pool.Get(e)

		pool.Put(ctx, balancer)

		got, ok := testPoolConnValue(pool, e.Key())
		require.True(t, ok)
		require.Equal(t, bootstrap, got.cc)
	})

	t.Run("PutDoesNotDeadlockWhenCloseCallsRemove", func(t *testing.T) {
		ctx := context.Background()
		pool := NewPool(ctx, &mockConfig{})
		defer func() {
			_ = pool.RemoveRef(ctx)
		}()

		e := endpoint.New("deadlock:2135")

		var lockFreeDuringClose atomic.Bool
		cc := newConn(e, pool,
			withOnClose(func(c *conn) {
				if pool.mu.TryLock() {
					lockFreeDuringClose.Store(true)
					pool.mu.Unlock()
				}
			}),
		)

		pool.mu.Lock()
		value := &connValue{cc: cc}
		value.useCount = 1
		pool.conns[e.Key()] = value
		pool.mu.Unlock()

		done := make(chan struct{})
		go func() {
			defer close(done)
			pool.Put(ctx, cc)
		}()

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("deadlock: Put blocked while conn.Close waits for pool mutex")
		}

		require.True(t, lockFreeDuringClose.Load(),
			"pool mutex must be released before conn.Close invokes remove",
		)
		require.False(t, testPoolHasConn(pool, e.Key()))
	})

	t.Run("PutCloseDoesNotDeadlockWithConcurrentGet", func(t *testing.T) {
		ctx := context.Background()
		pool := NewPool(ctx, &mockConfig{})
		defer func() {
			_ = pool.RemoveRef(ctx)
		}()

		const workers = 16
		const iterations = 64

		e := endpoint.New("concurrent-deadlock:2135")
		var wg sync.WaitGroup
		wg.Add(workers)

		for range workers {
			go func() {
				defer wg.Done()
				for range iterations {
					c := pool.Get(e)
					pool.Put(ctx, c)
				}
			}()
		}

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("deadlock: concurrent Get/Put did not complete")
		}
	})

	t.Run("GetCreatesFreshConnAfterPutClose", func(t *testing.T) {
		ctx := context.Background()
		pool := NewPool(ctx, &mockConfig{})
		defer func() {
			_ = pool.RemoveRef(ctx)
		}()

		e := endpoint.New("race-endpoint:2135")
		first := pool.Get(e)
		require.NotNil(t, first)

		pool.Put(ctx, first)
		require.False(t, testPoolHasConn(pool, e.Key()))

		second := pool.Get(e)
		require.NotNil(t, second)
		require.NotSame(t, first, second)
		require.True(t, testPoolHasConn(pool, e.Key()))
	})

	t.Run("GetAndPutAreAtomicUnderMutex", func(t *testing.T) {
		ctx := context.Background()
		pool := NewPool(ctx, &mockConfig{})
		defer func() {
			_ = pool.RemoveRef(ctx)
		}()

		e := endpoint.New("atomic:2135")
		const workers = 16
		const iterations = 128

		var wg sync.WaitGroup
		wg.Add(workers)

		for range workers {
			go func() {
				defer wg.Done()
				for range iterations {
					c := pool.Get(e)
					require.NotNil(t, c)
					require.False(t, c.(*conn).isClosed())
					pool.Put(ctx, c)
				}
			}()
		}

		wg.Wait()

		useCount, hasConn := testPoolConnUseCount(pool, e.Key())
		require.False(t, hasConn)
		require.Equal(t, int64(0), useCount)
	})

	t.Run("GetReturnsNilWhenPoolClosed", func(t *testing.T) {
		ctx := context.Background()
		pool := NewPool(ctx, &mockConfig{})

		e := endpoint.New("closed-pool:2135")
		require.NotNil(t, pool.Get(e))

		require.NoError(t, pool.RemoveRef(ctx))
		require.True(t, pool.closed.Load())
		require.Nil(t, pool.Get(e))
	})

	t.Run("GetDoesNotPanicConcurrentWithRemoveRef", func(t *testing.T) {
		ctx := context.Background()
		pool := NewPool(ctx, &mockConfig{})

		e := endpoint.New("get-remove-race:2135")

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			for range 128 {
				_ = pool.Get(e)
			}
		}()

		go func() {
			defer wg.Done()
			_ = pool.RemoveRef(ctx)
		}()

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("Get/RemoveRef race did not finish")
		}
	})

	t.Run("AddRefAfterRemoveRefReturnsError", func(t *testing.T) {
		ctx := context.Background()
		pool := NewPool(ctx, &mockConfig{})

		require.NoError(t, pool.RemoveRef(ctx))
		require.Error(t, pool.AddRef(ctx))
	})

	t.Run("PutExtraCallsDoNotUnderflowUseCount", func(t *testing.T) {
		ctx := context.Background()
		pool := NewPool(ctx, &mockConfig{})
		defer func() {
			_ = pool.RemoveRef(ctx)
		}()

		e := endpoint.New("underflow:2135")
		c := pool.Get(e)

		pool.Put(ctx, c)
		require.False(t, testPoolHasConn(pool, e.Key()))

		pool.Put(ctx, c)
		pool.Put(ctx, c)
		require.False(t, testPoolHasConn(pool, e.Key()))
	})

	t.Run("GetProceedsWhilePutClosesOutsidePoolMutex", func(t *testing.T) {
		ctx := context.Background()
		pool := NewPool(ctx, &mockConfig{})
		defer func() {
			_ = pool.RemoveRef(ctx)
		}()

		e := endpoint.New("slow-close:2135")
		closeStarted := make(chan struct{})
		closeContinue := make(chan struct{})

		cc := newConn(e, pool,
			withOnClose(func(c *conn) {
				close(closeStarted)
				<-closeContinue
			}),
		)

		pool.mu.Lock()
		value := &connValue{cc: cc}
		value.useCount = 1
		pool.conns[e.Key()] = value
		pool.mu.Unlock()

		putDone := make(chan struct{})
		go func() {
			defer close(putDone)
			pool.Put(ctx, cc)
		}()

		select {
		case <-closeStarted:
		case <-time.After(2 * time.Second):
			t.Fatal("Put did not reach conn.Close")
		}

		got := pool.Get(e)
		require.NotNil(t, got)
		require.NotSame(t, cc, got)
		require.False(t, got.(*conn).isClosed())

		close(closeContinue)

		select {
		case <-putDone:
		case <-time.After(2 * time.Second):
			t.Fatal("Put did not finish")
		}
	})

	t.Run("ReleaseDoesNotBlockIsClosed", func(t *testing.T) {
		ctx := context.Background()
		pool := NewPool(ctx, &mockConfig{})

		for i := range 8 {
			_ = pool.Get(endpoint.New("release-block:2135", endpoint.WithID(uint32(i+1))))
		}

		releaseDone := make(chan struct{})
		go func() {
			defer close(releaseDone)
			_ = pool.RemoveRef(ctx)
		}()

		deadline := time.After(2 * time.Second)
		for !pool.closed.Load() {
			select {
			case <-deadline:
				t.Fatal("isClosed stayed blocked while RemoveRef closes connections")
			case <-time.After(10 * time.Millisecond):
			}
		}

		select {
		case <-releaseDone:
		case <-time.After(5 * time.Second):
			t.Fatal("RemoveRef did not finish")
		}
	})

	t.Run("GetIncrementsUseCountButEndpointsToConnectionsDoesNot", func(t *testing.T) {
		ctx := context.Background()
		pool := NewPool(ctx, &mockConfig{})
		defer func() {
			_ = pool.RemoveRef(ctx)
		}()

		e := endpoint.New("refcount-api:2135")

		_ = endpointsToConnections(pool, []endpoint.Endpoint{e})
		useCount, ok := testPoolConnUseCount(pool, e.Key())
		require.True(t, ok)
		require.Equal(t, int64(0), useCount)

		c := pool.Get(e)
		require.NotNil(t, c)

		useCount, ok = testPoolConnUseCount(pool, e.Key())
		require.True(t, ok)
		require.Equal(t, int64(1), useCount)
	})

	t.Run("ConcurrentGetPutDoesNotResurrectClosedConn", func(t *testing.T) {
		e := endpoint.New("resurrect:2135")
		var closedConns atomic.Int64

		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)

		ctx := context.Background()
		pool := NewPool(ctx, &mockConfig{})
		defer func() {
			close(stop)
			wg.Wait()
			_ = pool.RemoveRef(ctx)
		}()

		for range 2 {
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
						c := pool.Get(e)
						if c == nil {
							continue
						}
						if c.(*conn).isClosed() {
							closedConns.Add(1)
						}
						pool.Put(ctx, c)
					}
				}
			}()
		}

		time.Sleep(500 * time.Millisecond)

		require.Equal(t, int64(0), closedConns.Load())
	})
}

func TestMatchesResolveTarget(t *testing.T) {
	require.True(t, matchesResolveTarget("localhost:2135", "localhost:2135"))
	require.True(t, matchesResolveTarget("localhost:2135", "localhost"))
	require.False(t, matchesResolveTarget("localhost:2135", "other:2135"))
}

func TestPool_CloseConnsForFailedResolvePreservesPoolEntry(t *testing.T) {
	ctx := context.Background()
	pool := NewPool(ctx, &mockConfig{})
	defer func() {
		_ = pool.RemoveRef(ctx)
	}()

	target := "localhost:2135"
	e := endpoint.New(target)
	_ = pool.Get(e)

	useCount, ok := testPoolConnUseCount(pool, e.Key())
	require.True(t, ok)
	require.Equal(t, int64(1), useCount)

	pool.closeConnsForFailedResolve(ctx, target)

	require.True(t, testPoolHasConn(pool, e.Key()))
	useCount, ok = testPoolConnUseCount(pool, e.Key())
	require.True(t, ok)
	require.Equal(t, int64(1), useCount)
}

func TestPool_CloseConnsForFailedResolveReleasesPoolMutexAfterSnapshot(t *testing.T) {
	ctx := context.Background()
	pool := NewPool(ctx, &mockConfig{})
	t.Cleanup(func() {
		_ = pool.RemoveRef(ctx)
	})

	target := "localhost:2135"
	e := endpoint.New(target)
	_ = pool.Get(e)

	pool.closeConnsForFailedResolve(ctx, target)

	require.True(t, pool.mu.TryLock(), "pool mutex must be released after snapshot")
	pool.mu.Unlock()
}

func TestPool_RemoveRefSetsClosedUnderMutex(t *testing.T) {
	ctx := context.Background()
	pool := NewPool(ctx, &mockConfig{})

	require.NoError(t, pool.RemoveRef(ctx))
	require.True(t, pool.closed.Load())

	var wg sync.WaitGroup
	wg.Add(8)
	for range 8 {
		go func() {
			defer wg.Done()
			_ = pool.RemoveRef(ctx)
		}()
	}
	wg.Wait()

	require.True(t, pool.closed.Load())
}

func TestPool_PutIgnoresInvalidConn(t *testing.T) {
	ctx := context.Background()
	pool := NewPool(ctx, &mockConfig{})
	defer func() {
		_ = pool.RemoveRef(ctx)
	}()

	var typedNil *conn
	var asConn Conn = typedNil
	pool.Put(ctx, asConn)
	pool.Put(ctx, &mock.Conn{})
}

func TestPool_Ban(t *testing.T) {
	ctx := context.Background()
	pool := NewPool(ctx, &mockConfig{})

	e := endpoint.New("ban:2135")
	c := pool.Get(e)
	require.NotNil(t, c)

	pool.Ban(ctx, c, errors.New("overload"))
	require.Equal(t, state.Banned, c.State())

	require.NoError(t, pool.RemoveRef(ctx))
	require.NotPanics(t, func() {
		pool.Ban(ctx, c, errors.New("after close"))
	})
}

func TestPool_PutAfterRemoveRefClosesOutsideMap(t *testing.T) {
	ctx := context.Background()
	pool := NewPool(ctx, &mockConfig{})

	e := endpoint.New("put-after-close:2135")
	c := pool.Get(e)
	require.NoError(t, pool.RemoveRef(ctx))

	pool.Put(ctx, c)
	require.False(t, testPoolHasConn(pool, e.Key()))
}

func TestPool_OnResolveDone(t *testing.T) {
	ctx := context.Background()
	pool := NewPool(ctx, &mockConfig{})
	defer func() {
		_ = pool.RemoveRef(ctx)
	}()

	target := "localhost:2135"
	e := endpoint.New(target)
	_ = pool.Get(e)

	pool.onResolveDone(ctx, target, []string{target}, trace.DriverResolveDoneInfo{
		Error: errors.New("resolve failed"),
	})
	require.True(t, testPoolHasConn(pool, e.Key()))

	useCount, ok := testPoolConnUseCount(pool, e.Key())
	require.True(t, ok)
	require.Equal(t, int64(1), useCount)

	pool.onResolveDone(ctx, target, nil, trace.DriverResolveDoneInfo{})
	require.True(t, testPoolHasConn(pool, e.Key()))
}

func TestPool_ConnsMatchingResolveTargetNilMap(t *testing.T) {
	ctx := context.Background()
	pool := NewPool(ctx, &mockConfig{})
	require.NoError(t, pool.RemoveRef(ctx))

	require.Nil(t, pool.connsMatchingResolveTarget("localhost:2135"))
}

func TestPool_EndpointsToConnectionsNilMap(t *testing.T) {
	ctx := context.Background()
	pool := NewPool(ctx, &mockConfig{})
	require.NoError(t, pool.RemoveRef(ctx))

	e := endpoint.New("closed:2135")
	require.Empty(t, endpointsToConnections(pool, []endpoint.Endpoint{e}))
}

func TestPool_AddRefReturnsErrorWhenClosed(t *testing.T) {
	ctx := context.Background()
	pool := NewPool(ctx, &mockConfig{})
	require.NoError(t, pool.RemoveRef(ctx))

	err := pool.AddRef(ctx)
	require.ErrorIs(t, err, ErrClosedPool)
}

func TestPool_RemoveRefReturnsCloseErrors(t *testing.T) {
	ctx := context.Background()
	pool := NewPool(ctx, &mockConfig{})

	e := endpoint.New("close-error:2135")
	cc := newConn(e, pool)
	cc.mtx.Lock()
	cc.grpcConn = &mockGrpcConn{closeErr: errors.New("grpc close failed")}
	cc.mtx.Unlock()

	pool.mu.Lock()
	pool.conns[e.Key()] = &connValue{cc: cc}
	pool.mu.Unlock()

	err := pool.RemoveRef(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "connection pool close failed")
}

func TestPool_OnResolveCallback(t *testing.T) {
	ctx := context.Background()
	pool := NewPool(ctx, &mockConfig{})
	defer func() {
		_ = pool.RemoveRef(ctx)
	}()

	target := "localhost:2135"
	e := endpoint.New(target)
	_ = pool.Get(e)

	done := pool.onResolveCallback(ctx, trace.DriverResolveStartInfo{
		Target:   target,
		Resolved: []string{target},
	})
	done(trace.DriverResolveDoneInfo{Error: errors.New("resolve failed")})

	require.True(t, testPoolHasConn(pool, e.Key()))
}

func TestPool_CloseConnsForFailedResolveSkipsAlreadyClosed(t *testing.T) {
	ctx := context.Background()
	pool := NewPool(ctx, &mockConfig{})
	defer func() {
		_ = pool.RemoveRef(ctx)
	}()

	target := "localhost:2135"
	e := endpoint.New(target)
	cc := newConn(e, pool)
	cc.mtx.Lock()
	cc.closed = true
	cc.mtx.Unlock()

	pool.mu.Lock()
	pool.conns[e.Key()] = &connValue{cc: cc}
	pool.mu.Unlock()

	require.NotPanics(t, func() {
		pool.closeConnsForFailedResolve(ctx, target)
	})
}

func TestPool_OnResolveDriverTrace(t *testing.T) {
	ctx := context.Background()
	pool := NewPool(ctx, &mockConfig{})
	defer func() {
		_ = pool.RemoveRef(ctx)
	}()

	target := "localhost:2135"
	e := endpoint.New(target)
	_ = pool.Get(e)

	onDone := gtrace.DriverOnResolve(
		pool.onResolveDriverTrace(ctx, (&mockConfig{}).Trace()),
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.TestPool_OnResolveDriverTrace"),
		target,
		nil,
	)
	onDone(errors.New("resolve failed"))

	require.True(t, testPoolHasConn(pool, e.Key()))
}

type mockGrpcConn struct {
	closeErr error
}

func (m *mockGrpcConn) Invoke(context.Context, string, any, any, ...grpc.CallOption) error {
	return nil
}

func (m *mockGrpcConn) NewStream(
	context.Context,
	*grpc.StreamDesc,
	string,
	...grpc.CallOption,
) (grpc.ClientStream, error) {
	return nil, errors.New("not implemented")
}

func (m *mockGrpcConn) Close() error {
	return m.closeErr
}

func (m *mockGrpcConn) GetState() connectivity.State {
	return connectivity.Ready
}
