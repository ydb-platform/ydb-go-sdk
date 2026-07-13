package balancer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
)

func TestBalancer_ReviewConcerns(t *testing.T) {
	t.Run("NextStateFiltersNilFromClosedPool", func(t *testing.T) {
		ctx := context.Background()
		pool := conn.NewPool(ctx, config.New())
		require.NoError(t, pool.Release(ctx))

		newQuarantine, newActive := nextState(ctx, pool, nil, nil, []endpoint.Endpoint{
			endpoint.New("node:2135", endpoint.WithID(1)),
		})

		require.Empty(t, newQuarantine)
		require.Empty(t, newActive)
	})

	t.Run("ApplyDiscoveredEndpointsOnClosedPoolDoesNotPanic", func(t *testing.T) {
		ctx := context.Background()
		pool := conn.NewPool(ctx, config.New())
		require.NoError(t, pool.Release(ctx))

		b := &Balancer{
			driverConfig:   config.New(),
			pool:           pool,
			balancerConfig: balancerConfig.Config{},
		}

		require.NotPanics(t, func() {
			b.applyDiscoveredEndpoints(ctx, []endpoint.Endpoint{
				endpoint.New("node:2135", endpoint.WithID(1)),
			}, "")
		})
	})

	t.Run("InvokeAfterCloseReturnsBalancerClosed", func(t *testing.T) {
		ctx := context.Background()
		cfg := config.New()
		pool := conn.NewPool(ctx, cfg)
		defer func() { _ = pool.Release(ctx) }()

		cc := &mock.Conn{
			ClientConnInterface: &grpc.ClientConn{},
			AddrField:           "node:2135",
			NodeIDField:         1,
		}

		b := &Balancer{
			driverConfig:   cfg,
			pool:           pool,
			balancerConfig: balancerConfig.Config{},
		}
		b.connectionsState.Store(newConnectionsState(
			[]conn.Conn{cc},
			nil,
			balancerConfig.Info{},
			true,
			nil,
		))

		require.NoError(t, b.Close(ctx))

		err := b.Invoke(ctx, "/test.Service/Method", nil, nil)
		require.Error(t, err)
		require.True(t, errors.Is(err, errBalancerClosed))
	})

	t.Run("CloseUsesWriteLockAndIsIdempotent", func(t *testing.T) {
		ctx := context.Background()
		cfg := config.New()
		pool := conn.NewPool(ctx, cfg)
		defer func() { _ = pool.Release(ctx) }()

		b := &Balancer{
			driverConfig:   cfg,
			pool:           pool,
			balancerConfig: balancerConfig.Config{},
		}
		b.connectionsState.Store(newConnectionsState(nil,
			nil, balancerConfig.Info{}, true, nil,
		))

		var wg sync.WaitGroup
		errs := make(chan error, 8)
		wg.Add(8)

		for range 8 {
			go func() {
				defer wg.Done()
				errs <- b.Close(ctx)
			}()
		}

		wg.Wait()
		close(errs)

		var closedErrs int
		for err := range errs {
			if errors.Is(err, errBalancerClosed) {
				closedErrs++
			} else {
				require.NoError(t, err)
			}
		}

		require.Equal(t, 7, closedErrs)
		require.True(t, b.closed)
	})

	t.Run("ApplyDiscoveredEndpointsDuringCloseDoesNotPanic", func(t *testing.T) {
		ctx := context.Background()
		cfg := config.New()
		pool := conn.NewPool(ctx, cfg)
		defer func() { _ = pool.Release(ctx) }()

		b := &Balancer{
			driverConfig:   cfg,
			pool:           pool,
			balancerConfig: balancerConfig.Config{},
		}
		b.connectionsState.Store(newConnectionsState(nil,
			nil, balancerConfig.Info{}, true, nil,
		))

		closeStarted := make(chan struct{})
		go func() {
			close(closeStarted)
			_ = b.Close(ctx)
		}()

		<-closeStarted
		time.Sleep(10 * time.Millisecond)

		require.NotPanics(t, func() {
			b.applyDiscoveredEndpoints(ctx, []endpoint.Endpoint{
				endpoint.New("late-discovery:2135", endpoint.WithID(1)),
			}, "")
		})
	})
}
