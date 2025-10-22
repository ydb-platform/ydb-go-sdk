package conn

import (
	"context"
	"testing"
	"time"

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
	dialTimeout    time.Duration
	connectionTTL  time.Duration
	driverTrace    *trace.Driver
	grpcDialOpts   []grpc.DialOption
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
		opts := []grpc.DialOption{grpc.WithBlock()}
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
