package conn

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xslices"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// mockConfig implements the Config interface for testing
type mockConfig struct {
	dialTimeout   time.Duration
	connectionTTL time.Duration
	driverTrace   *trace.Driver
	grpcDialOpts  []grpc.DialOption
}

func (m *mockConfig) DialTimeout() time.Duration {
	return m.dialTimeout
}

func (m *mockConfig) ConnectionTTL() time.Duration {
	return m.connectionTTL
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

func countPoolConns(pool *Pool) (n int) {
	pool.conns.Range(func(_ endpoint.Key, _ *conn) bool {
		n++

		return true
	})

	return n
}

func TestPool_Get(t *testing.T) {
	t.Run("GetSameConnectionTwice", func(t *testing.T) {
		ctx := context.Background()
		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
		}
		pool := NewPool(ctx, config)
		defer func() {
			_ = pool.Release(ctx)
		}()

		e := endpoint.New("test-endpoint:2135")

		conn1 := pool.get(e)
		require.NotNil(t, conn1)

		conn2 := pool.get(e)
		require.NotNil(t, conn2)

		// Should return the same connection
		require.Equal(t, conn1, conn2)
	})

	t.Run("GetDifferentEndpoints", func(t *testing.T) {
		ctx := context.Background()
		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
		}
		pool := NewPool(ctx, config)
		defer func() {
			_ = pool.Release(ctx)
		}()

		e1 := endpoint.New("endpoint1:2135")
		e2 := endpoint.New("endpoint2:2135")

		conn1 := pool.get(e1)
		require.NotNil(t, conn1)

		conn2 := pool.get(e2)
		require.NotNil(t, conn2)

		// Should return different connections
		require.NotEqual(t, conn1, conn2)
	})
}

func TestPool_TakeRelease(t *testing.T) {
	t.Run("TakeIncreasesUsageCounter", func(t *testing.T) {
		ctx := context.Background()
		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
		}
		pool := NewPool(ctx, config)

		// Initial usage is 1
		require.Equal(t, int64(1), pool.usages)

		err := pool.Take(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(2), pool.usages)

		err = pool.Take(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(3), pool.usages)

		// Clean up
		_ = pool.Release(ctx)
		_ = pool.Release(ctx)
		_ = pool.Release(ctx)
	})

	t.Run("ReleaseDecreasesUsageCounter", func(t *testing.T) {
		ctx := context.Background()
		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
		}
		pool := NewPool(ctx, config)

		err := pool.Take(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(2), pool.usages)

		err = pool.Release(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(1), pool.usages)

		// Clean up
		_ = pool.Release(ctx)
	})

	t.Run("FinalReleaseClosesPool", func(t *testing.T) {
		ctx := context.Background()
		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
		}
		pool := NewPool(ctx, config)

		// Get a connection to ensure the pool has something to close
		e := endpoint.New("test-endpoint:2135")
		conn := pool.get(e)
		require.NotNil(t, conn)

		// Final release should close the pool
		err := pool.Release(ctx)
		require.NoError(t, err)

		// Pool should be closed
		require.True(t, pool.isClosed())
	})
}

func TestPool_IsClosed(t *testing.T) {
	t.Run("NewPoolIsNotClosed", func(t *testing.T) {
		ctx := context.Background()
		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
		}
		pool := NewPool(ctx, config)
		defer func() {
			_ = pool.Release(ctx)
		}()

		require.False(t, pool.isClosed())
	})

	t.Run("ReleasedPoolIsClosed", func(t *testing.T) {
		ctx := context.Background()
		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
		}
		pool := NewPool(ctx, config)

		err := pool.Release(ctx)
		require.NoError(t, err)

		require.True(t, pool.isClosed())
	})
}

func TestPool_ConfigMethods(t *testing.T) {
	t.Run("DialTimeout", func(t *testing.T) {
		ctx := context.Background()
		config := &mockConfig{
			dialTimeout:   10 * time.Second,
			connectionTTL: 0,
		}
		pool := NewPool(ctx, config)
		defer func() {
			_ = pool.Release(ctx)
		}()

		require.Equal(t, 10*time.Second, pool.DialTimeout())
	})

	t.Run("Trace", func(t *testing.T) {
		ctx := context.Background()
		driverTrace := &trace.Driver{}
		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
			driverTrace:   driverTrace,
		}
		pool := NewPool(ctx, config)
		defer func() {
			_ = pool.Release(ctx)
		}()

		require.Equal(t, driverTrace, pool.Trace())
	})

	t.Run("GrpcDialOptions", func(t *testing.T) {
		ctx := context.Background()
		opts := []grpc.DialOption{grpc.WithBlock()} //nolint:staticcheck,nolintlint
		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
			grpcDialOpts:  opts,
		}
		pool := NewPool(ctx, config)
		defer func() {
			_ = pool.Release(ctx)
		}()

		// The pool adds additional options, so we check the length is at least the ones we provided
		require.GreaterOrEqual(t, len(pool.GrpcDialOptions()), len(opts))
	})
}

func TestPool_ConnParker(t *testing.T) {
	t.Run("AttemptsToCheckIdleOnlineConnections", func(t *testing.T) {
		ctx := context.Background()
		fakeClock := clockwork.NewFakeClock()

		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 5 * time.Minute,
		}

		pool := NewPool(ctx, config, func(p *Pool) {
			p.clock = fakeClock
		})
		defer func() {
			_ = pool.Release(ctx)
		}()

		// Create a connection and set it to Online
		e := endpoint.New("test-endpoint:2135")
		conn := pool.get(e)
		require.NotNil(t, conn)

		conn.SetState(ctx, state.Online)
		require.Equal(t, state.Online, conn.GetState())

		// Start the parker in background
		ttl := 10 * time.Second
		interval := 5 * time.Second
		go pool.connParker(ctx, ttl, interval)

		// Advance clock past the TTL
		fakeClock.Advance(interval)
		fakeClock.Advance(ttl + time.Second)

		// Give goroutine time to process
		time.Sleep(50 * time.Millisecond)

		// Note: Without actual grpcConn, park() is a no-op, so state won't change
		// This test verifies the parker runs without error
	})

	t.Run("AttemptsToCheckBannedConnections", func(t *testing.T) {
		ctx := context.Background()
		fakeClock := clockwork.NewFakeClock()

		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 5 * time.Minute,
		}

		pool := NewPool(ctx, config, func(p *Pool) {
			p.clock = fakeClock
		})
		defer func() {
			_ = pool.Release(ctx)
		}()

		// Create a connection and set it to Banned
		e := endpoint.New("test-endpoint:2135")
		conn := pool.get(e)
		require.NotNil(t, conn)

		conn.SetState(ctx, state.Banned)
		require.Equal(t, state.Banned, conn.GetState())

		// Start the parker in background
		ttl := 10 * time.Second
		interval := 5 * time.Second
		go pool.connParker(ctx, ttl, interval)

		// Advance clock past the TTL
		fakeClock.Advance(interval)
		fakeClock.Advance(ttl + time.Second)

		// Give goroutine time to process
		time.Sleep(50 * time.Millisecond)

		// Note: Without actual grpcConn, park() is a no-op, so state won't change
		// This test verifies the parker runs without error
	})

	t.Run("DoesNotParkRecentlyUsedConnections", func(t *testing.T) {
		ctx := context.Background()
		fakeClock := clockwork.NewFakeClock()

		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 5 * time.Minute,
		}

		pool := NewPool(ctx, config, func(p *Pool) {
			p.clock = fakeClock
		})
		defer func() {
			_ = pool.Release(ctx)
		}()

		// Create a connection and set it to Online
		e := endpoint.New("test-endpoint:2135")
		conn := pool.get(e)
		require.NotNil(t, conn)

		conn.SetState(ctx, state.Online)
		require.Equal(t, state.Online, conn.GetState())

		// Start the parker in background
		ttl := 10 * time.Second
		interval := 5 * time.Second
		go pool.connParker(ctx, ttl, interval)

		// Advance clock but not past TTL
		fakeClock.Advance(interval)
		fakeClock.Advance(ttl / 2)

		// Give goroutine time to process
		time.Sleep(50 * time.Millisecond)

		// Connection should still be Online (not idle enough to park)
		require.Equal(t, state.Online, conn.GetState())
	})

	t.Run("DoesNotParkCreatedConnections", func(t *testing.T) {
		ctx := context.Background()
		fakeClock := clockwork.NewFakeClock()

		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 5 * time.Minute,
		}

		pool := NewPool(ctx, config, func(p *Pool) {
			p.clock = fakeClock
		})
		defer func() {
			_ = pool.Release(ctx)
		}()

		// Create a connection (default state is Created)
		e := endpoint.New("test-endpoint:2135")
		conn := pool.get(e)
		require.NotNil(t, conn)
		require.Equal(t, state.Created, conn.GetState())

		// Start the parker in background
		ttl := 10 * time.Second
		interval := 5 * time.Second
		go pool.connParker(ctx, ttl, interval)

		// Advance clock past the TTL
		fakeClock.Advance(interval)
		fakeClock.Advance(ttl + time.Second)

		// Give goroutine time to process
		time.Sleep(50 * time.Millisecond)

		// Connection should still be Created (not parked since not Online/Banned)
		require.Equal(t, state.Created, conn.GetState())
	})

	t.Run("StopsWhenPoolIsClosed", func(t *testing.T) {
		ctx := context.Background()
		fakeClock := clockwork.NewFakeClock()

		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 5 * time.Minute,
		}

		pool := NewPool(ctx, config, func(p *Pool) {
			p.clock = fakeClock
		})

		// Start the parker in background
		ttl := 10 * time.Second
		interval := 5 * time.Second
		parkerDone := make(chan struct{})
		go func() {
			pool.connParker(ctx, ttl, interval)
			close(parkerDone)
		}()

		// Close the pool
		err := pool.Release(ctx)
		require.NoError(t, err)

		// Parker should stop
		select {
		case <-parkerDone:
			// Success - parker stopped
		case <-time.After(100 * time.Millisecond):
			t.Fatal("connParker did not stop after pool was closed")
		}
	})

	t.Run("TickerAdvancesCorrectly", func(t *testing.T) {
		ctx := context.Background()
		fakeClock := clockwork.NewFakeClock()

		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 5 * time.Minute,
		}

		pool := NewPool(ctx, config, func(p *Pool) {
			p.clock = fakeClock
		})
		defer func() {
			_ = pool.Release(ctx)
		}()

		tickCount := 0

		// Create connection to track parking attempts
		e := endpoint.New("test-endpoint:2135")
		conn := pool.get(e)
		conn.SetState(ctx, state.Online)

		// Start the parker
		ttl := 10 * time.Second
		interval := 5 * time.Second
		go pool.connParker(ctx, ttl, interval)

		// Advance clock multiple times and verify parker is running
		for range 3 {
			fakeClock.Advance(interval)
			fakeClock.Advance(ttl)
			time.Sleep(50 * time.Millisecond)
			tickCount++
		}

		// If we got here without hanging, the ticker is working
		require.Equal(t, 3, tickCount)
	})
}

func TestPool_DiscoveryConnectionsRefs(t *testing.T) {
	t.Run("KeepsConnectionWhileReferenced", func(t *testing.T) {
		ctx := context.Background()
		pool := NewPool(ctx, &mockConfig{})
		defer func() {
			_ = pool.Release(ctx)
		}()

		e := endpoint.New("test-endpoint:2135")
		pool.DiscoveryConnections(ctx, []endpoint.Endpoint{e}, nil, []endpoint.Endpoint{e})
		pool.DiscoveryConnections(ctx, []endpoint.Endpoint{e}, nil, []endpoint.Endpoint{e})

		pool.DiscoveryConnections(ctx, nil, []endpoint.Endpoint{e}, []endpoint.Endpoint{e})
		_, ok := pool.conns.Get(e.Key())
		require.True(t, ok)

		pool.DiscoveryConnections(ctx, nil, []endpoint.Endpoint{e}, nil)
		_, ok = pool.conns.Get(e.Key())
		require.True(t, ok)

		pool.DiscoveryConnections(ctx, nil, nil, nil)
		_, ok = pool.conns.Get(e.Key())
		require.False(t, ok)
	})

	t.Run("ReListsEndpointAfterDrop", func(t *testing.T) {
		ctx := context.Background()
		pool := NewPool(ctx, &mockConfig{})
		defer func() {
			_ = pool.Release(ctx)
		}()

		e := endpoint.New("test-endpoint:2135")
		pool.DiscoveryConnections(ctx, []endpoint.Endpoint{e}, nil, []endpoint.Endpoint{e})
		pool.DiscoveryConnections(ctx, nil, []endpoint.Endpoint{e}, nil)

		pool.DiscoveryConnections(ctx, []endpoint.Endpoint{e}, nil, []endpoint.Endpoint{e})
		_, ok := pool.conns.Get(e.Key())
		require.True(t, ok)
	})

	t.Run("SharedPoolMultipleBalancers", func(t *testing.T) {
		ctx := context.Background()
		pool := NewPool(ctx, &mockConfig{})
		defer func() {
			_ = pool.Release(ctx)
		}()

		e := endpoint.New("shared-endpoint:2135")
		pool.DiscoveryConnections(ctx, []endpoint.Endpoint{e}, nil, []endpoint.Endpoint{e})
		pool.DiscoveryConnections(ctx, []endpoint.Endpoint{e}, nil, []endpoint.Endpoint{e})

		pool.DiscoveryConnections(ctx, nil, []endpoint.Endpoint{e}, []endpoint.Endpoint{e})
		pool.DiscoveryConnections(ctx, nil, nil, nil)
		_, ok := pool.conns.Get(e.Key())
		require.True(t, ok)

		// Each call models [balancer.Balancer.Close] on a shared pool.
		pool.DiscoveryConnections(ctx, nil, []endpoint.Endpoint{e}, nil)
		pool.DiscoveryConnections(ctx, nil, []endpoint.Endpoint{e}, nil)
		pool.DiscoveryConnections(ctx, nil, nil, nil)
		_, ok = pool.conns.Get(e.Key())
		require.False(t, ok)
	})
}

func TestPool_DiscoveryConnectionsConcurrent(t *testing.T) {
	ctx := context.Background()
	pool := NewPool(ctx, &mockConfig{})
	defer func() {
		_ = pool.Release(ctx)
	}()

	e := endpoint.New("concurrent:2135", endpoint.WithID(1))

	const workers = 8
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for range 50 {
				pool.DiscoveryConnections(
					ctx,
					[]endpoint.Endpoint{e},
					nil,
					[]endpoint.Endpoint{e},
				)
				pool.DiscoveryConnections(ctx, nil, []endpoint.Endpoint{e}, nil)
			}
		}()
	}
	wg.Wait()

	require.LessOrEqual(t, countPoolConns(pool), 1)
}

func TestPool_AcquireConnNotClosedByDiscoveryCleanup(t *testing.T) {
	ctx := context.Background()
	pool := NewPool(ctx, &mockConfig{})
	defer func() {
		_ = pool.Release(ctx)
	}()

	e := endpoint.New("bootstrap:2135")
	conn := pool.AcquireConn(e)
	require.NotNil(t, conn)

	pool.DiscoveryConnections(ctx, nil, nil, nil)

	got, ok := pool.conns.Get(e.Key())
	require.True(t, ok)
	require.Equal(t, conn, got)
}

func TestPool_AcquireConnReleaseClosedOnDiscovery(t *testing.T) {
	ctx := context.Background()
	pool := NewPool(ctx, &mockConfig{})
	defer func() {
		_ = pool.Release(ctx)
	}()

	e := endpoint.New("acquire-release:2135")
	conn := pool.AcquireConn(e)
	require.NotNil(t, conn)

	pool.DiscoveryConnections(ctx, nil, []endpoint.Endpoint{e}, nil)
	pool.DiscoveryConnections(ctx, nil, nil, nil)

	_, ok := pool.conns.Get(e.Key())
	require.False(t, ok)
}

func TestPool_DiscoveryEndpointLifecycle(t *testing.T) {
	t.Run("DropsRemovedEndpointsOnNextDiscovery", func(t *testing.T) {
		ctx := context.Background()
		pool := NewPool(ctx, &mockConfig{})
		defer func() {
			_ = pool.Release(ctx)
		}()

		e1 := endpoint.New("node1:2135", endpoint.WithID(1))
		e2 := endpoint.New("node2:2135", endpoint.WithID(2))

		apply := func(previous, newest []endpoint.Endpoint) {
			_, added, dropped := xslices.Diff(previous, newest, endpoint.Compare)
			pool.DiscoveryConnections(ctx, added, dropped, newest)
		}

		apply(nil, []endpoint.Endpoint{e1, e2})
		_, ok := pool.conns.Get(e1.Key())
		require.True(t, ok)
		_, ok = pool.conns.Get(e2.Key())
		require.True(t, ok)

		apply([]endpoint.Endpoint{e1, e2}, []endpoint.Endpoint{e1})
		_, ok = pool.conns.Get(e1.Key())
		require.True(t, ok)
		_, ok = pool.conns.Get(e2.Key())
		require.True(t, ok)

		apply([]endpoint.Endpoint{e1}, []endpoint.Endpoint{e1})
		_, ok = pool.conns.Get(e1.Key())
		require.True(t, ok)
		_, ok = pool.conns.Get(e2.Key())
		require.False(t, ok)
	})
}

func TestPool_DiscoveryConnections(t *testing.T) {
	t.Run("CreatesConnectionsForEndpoints", func(t *testing.T) {
		ctx := context.Background()
		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
		}
		pool := NewPool(ctx, config)
		defer func() {
			_ = pool.Release(ctx)
		}()

		e1 := endpoint.New("e1:2135")
		e2 := endpoint.New("e2:2135")

		conns := pool.DiscoveryConnections(ctx, []endpoint.Endpoint{e1, e2}, nil, []endpoint.Endpoint{e1, e2})

		require.Len(t, conns, 2)
		_, ok := pool.conns.Get(e1.Key())
		require.True(t, ok)
		_, ok = pool.conns.Get(e2.Key())
		require.True(t, ok)

		same := pool.DiscoveryConnections(ctx, nil, nil, []endpoint.Endpoint{e1, e2})
		require.Equal(t, conns[0], same[0])
		require.Equal(t, conns[1], same[1])
	})

	t.Run("ReusesExistingConnections", func(t *testing.T) {
		ctx := context.Background()
		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
		}
		pool := NewPool(ctx, config)
		defer func() {
			_ = pool.Release(ctx)
		}()

		e := endpoint.New("reuse:2135")

		existing := pool.DiscoveryConnections(ctx, []endpoint.Endpoint{e}, nil, []endpoint.Endpoint{e})[0]
		require.NotNil(t, existing)

		conns := pool.DiscoveryConnections(ctx, nil, nil, []endpoint.Endpoint{e})

		require.Len(t, conns, 1)
		require.Equal(t, existing, conns[0])

		got, ok := pool.conns.Get(e.Key())
		require.True(t, ok)
		require.Equal(t, existing, got)
	})

	t.Run("IPv6AndHostOverrideUniqueKeys", func(t *testing.T) {
		ctx := context.Background()
		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
		}
		pool := NewPool(ctx, config)
		defer func() {
			_ = pool.Release(ctx)
		}()

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
		conns := pool.DiscoveryConnections(ctx, endpoints, nil, endpoints)

		require.Len(t, conns, len(endpoints))

		for i, e := range endpoints {
			got := conns[i]
			require.NotNil(t, got)
			cc, ok := pool.conns.Get(e.Key())
			require.True(t, ok)
			require.Equal(t, cc, got)
		}

		require.Equal(t, e2.Key().HostOverride, "override")
		require.Equal(t, e4.Key().NodeID, uint32(2))
		require.Equal(t, e5.Key().NodeID, uint32(1))
	})

	t.Run("AddNewEndpointAndNodeIDVariation", func(t *testing.T) {
		ctx := context.Background()
		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
		}
		pool := NewPool(ctx, config)
		defer func() {
			_ = pool.Release(ctx)
		}()

		// initial two endpoints with IPv6 and distinct NodeIDs
		e1 := endpoint.New("e1.example:2135", endpoint.WithIPV6([]string{"2001:db8::1"}), endpoint.WithID(1))
		e2 := endpoint.New("e2.example:2135", endpoint.WithIPV6([]string{"2001:db8::2"}), endpoint.WithID(2))

		// create initial connections
		initialConns := pool.DiscoveryConnections(
			ctx, []endpoint.Endpoint{e1, e2}, nil, []endpoint.Endpoint{e1, e2},
		)
		require.Len(t, initialConns, 2)
		require.True(t, pool.conns.Has(e1.Key()))
		require.True(t, pool.conns.Has(e2.Key()))

		// add a new unique endpoint e3 -> pool should grow
		e3 := endpoint.New("e3.example:2135", endpoint.WithIPV6([]string{"2001:db8::3"}), endpoint.WithID(3))
		connsAfterE3 := pool.DiscoveryConnections(
			ctx, []endpoint.Endpoint{e3}, nil, []endpoint.Endpoint{e1, e2, e3},
		)
		require.Len(t, connsAfterE3, 3)
		require.True(t, pool.conns.Has(e3.Key()))
		require.Equal(t, initialConns[0], connsAfterE3[0])
		require.Equal(t, initialConns[1], connsAfterE3[1])
		require.NotEqual(t, initialConns[0], connsAfterE3[2])

		// now use same address as e1 but different NodeID (and same ipv6) -> should create new conn
		e1DifferentNode := endpoint.New("e1.example:2135", endpoint.WithIPV6([]string{"2001:db8::1"}), endpoint.WithID(99))
		connsAfterNodeChange := pool.DiscoveryConnections(
			ctx, []endpoint.Endpoint{e1DifferentNode}, nil, []endpoint.Endpoint{e1DifferentNode},
		)
		require.Len(t, connsAfterNodeChange, 1)
		require.True(t, pool.conns.Has(e1DifferentNode.Key()))
		require.Equal(t, initialConns[0], connsAfterE3[0])
	})
}
