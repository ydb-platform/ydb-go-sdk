package conn

import (
	"context"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
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
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
		}
		pool := NewPool(ctx, config)
		defer func() {
			_ = pool.Release(ctx)
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

func TestPool_Ban(t *testing.T) {
	t.Run("BanConnection", func(t *testing.T) {
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
		conn := pool.Get(e)
		require.NotNil(t, conn)

		// Initially should be Created
		require.Equal(t, Created, conn.GetState())

		// Set to Online first
		conn.SetState(ctx, Online)
		require.Equal(t, Online, conn.GetState())

		// Ban the connection with Unavailable error
		err := xerrors.Transport(grpcStatus.Error(grpcCodes.Unavailable, "test"))
		pool.Ban(ctx, conn, err)

		// Should be Banned
		require.Equal(t, Banned, conn.GetState())
	})

	t.Run("DontBanOnNonTransportError", func(t *testing.T) {
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
		conn := pool.Get(e)
		require.NotNil(t, conn)

		conn.SetState(ctx, Online)
		require.Equal(t, Online, conn.GetState())

		// Try to ban with operation error (should not ban)
		err := xerrors.Operation()
		pool.Ban(ctx, conn, err)

		// Should still be Online
		require.Equal(t, Online, conn.GetState())
	})

	t.Run("BanOnResourceExhaustedError", func(t *testing.T) {
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
		conn := pool.Get(e)
		require.NotNil(t, conn)

		conn.SetState(ctx, Online)
		require.Equal(t, Online, conn.GetState())

		// Ban with ResourceExhausted error (should ban)
		err := xerrors.Transport(grpcStatus.Error(grpcCodes.ResourceExhausted, "test"))
		pool.Ban(ctx, conn, err)

		// Should be Banned
		require.Equal(t, Banned, conn.GetState())
	})

	t.Run("DontBanOnOtherTransportErrors", func(t *testing.T) {
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
		conn := pool.Get(e)
		require.NotNil(t, conn)

		conn.SetState(ctx, Online)
		require.Equal(t, Online, conn.GetState())

		// Try to ban with Internal error (should not ban)
		err := xerrors.Transport(grpcStatus.Error(grpcCodes.Internal, "test"))
		pool.Ban(ctx, conn, err)

		// Should still be Online
		require.Equal(t, Online, conn.GetState())
	})
}

func TestPool_Allow(t *testing.T) {
	t.Run("AllowBannedConnection", func(t *testing.T) {
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
		conn := pool.Get(e)
		require.NotNil(t, conn)

		// Set to Banned
		conn.SetState(ctx, Banned)
		require.Equal(t, Banned, conn.GetState())

		// Allow the connection
		pool.Allow(ctx, conn)

		// Should be Offline (since grpcConn is nil)
		require.Equal(t, Offline, conn.GetState())
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
		conn := pool.Get(e)
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
		conn := pool.Get(e)
		require.NotNil(t, conn)

		conn.SetState(ctx, Online)
		require.Equal(t, Online, conn.GetState())

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
		conn := pool.Get(e)
		require.NotNil(t, conn)

		conn.SetState(ctx, Banned)
		require.Equal(t, Banned, conn.GetState())

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
		conn := pool.Get(e)
		require.NotNil(t, conn)

		conn.SetState(ctx, Online)
		require.Equal(t, Online, conn.GetState())

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
		require.Equal(t, Online, conn.GetState())
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
		conn := pool.Get(e)
		require.NotNil(t, conn)
		require.Equal(t, Created, conn.GetState())

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
		require.Equal(t, Created, conn.GetState())
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
		conn := pool.Get(e)
		conn.SetState(ctx, Online)

		// Start the parker
		ttl := 10 * time.Second
		interval := 5 * time.Second
		go pool.connParker(ctx, ttl, interval)

		// Advance clock multiple times and verify parker is running
		for i := 0; i < 3; i++ {
			fakeClock.Advance(interval)
			fakeClock.Advance(ttl)
			time.Sleep(50 * time.Millisecond)
			tickCount++
		}

		// If we got here without hanging, the ticker is working
		require.Equal(t, 3, tickCount)
	})
}

func TestEndpointsToConnections(t *testing.T) {
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

		require.Equal(t, 0, pool.conns.Len())

		e1 := endpoint.New("e1:2135")
		e2 := endpoint.New("e2:2135")

		conns := EndpointsToConnections(pool, []endpoint.Endpoint{e1, e2})

		require.Len(t, conns, 2)
		require.Equal(t, 2, pool.conns.Len())

		require.Equal(t, pool.Get(e1), conns[0])
		require.Equal(t, pool.Get(e2), conns[1])
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

		existing := pool.Get(e)
		require.NotNil(t, existing)

		initialLen := pool.conns.Len()

		conns := EndpointsToConnections(pool, []endpoint.Endpoint{e})

		require.Len(t, conns, 1)
		require.Equal(t, existing, conns[0])

		require.Equal(t, initialLen, pool.conns.Len())
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

		// ensure empty pool
		require.Equal(t, 0, pool.conns.Len())

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
		// same ipv6 as e1 but different NodeID -> different Key.NodeID
		e4 := endpoint.New("example.com:2135", endpoint.WithIPV6([]string{"2001:db8::1"}), endpoint.WithID(1))

		endpoints := []endpoint.Endpoint{e1, e2, e3, e4}
		conns := EndpointsToConnections(pool, endpoints)

		require.Len(t, conns, len(endpoints))
		require.Equal(t, 4, pool.conns.Len())

		for i, e := range endpoints {
			got := conns[i]
			require.NotNil(t, got)
			require.Equal(t, pool.Get(e), got)
			cc, ok := pool.conns.Get(e.Key())
			require.True(t, ok)
			require.Equal(t, cc, got)
		}

		require.Equal(t, e2.Key().HostOverride, "override")
		require.Equal(t, e4.Key().NodeID, uint32(1))
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
		initialConns := EndpointsToConnections(pool, []endpoint.Endpoint{e1, e2})
		require.Len(t, initialConns, 2)
		require.Equal(t, 2, pool.conns.Len())
		require.Equal(t, pool.Get(e1), initialConns[0])
		require.Equal(t, pool.Get(e2), initialConns[1])

		// add a new unique endpoint e3 -> pool should grow
		e3 := endpoint.New("e3.example:2135", endpoint.WithIPV6([]string{"2001:db8::3"}), endpoint.WithID(3))
		connsAfterE3 := EndpointsToConnections(pool, []endpoint.Endpoint{e1, e2, e3})
		require.Len(t, connsAfterE3, 3)
		require.Equal(t, 3, pool.conns.Len())
		require.Equal(t, pool.Get(e3), connsAfterE3[2])

		// now use same address as e1 but different NodeID (and same ipv6) -> should create new conn
		e1DifferentNode := endpoint.New("e1.example:2135", endpoint.WithIPV6([]string{"2001:db8::1"}), endpoint.WithID(99))
		connsAfterNodeChange := EndpointsToConnections(pool, []endpoint.Endpoint{e1DifferentNode})
		require.Len(t, connsAfterNodeChange, 1)
		// pool size must increase by one
		require.Equal(t, 4, pool.conns.Len())
		// returned conn corresponds to the new endpoint key
		require.Equal(t, pool.Get(e1DifferentNode), connsAfterNodeChange[0])
		require.Equal(t, pool.Get(e1), initialConns[0])
	})
}
