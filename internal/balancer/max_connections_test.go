package balancer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/strategy"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
)

func TestMaxConnectionsWithNodeIDSoftlyExceedsLimit(t *testing.T) {
	ctx := context.Background()
	cfg := config.New()
	pool := conn.NewPool(ctx, cfg)
	balancer := &Balancer{
		driverConfig: cfg,
		balancer:     strategy.WithMaxConnections(strategy.RandomChoice(), 2),
		pool:         pool,
		rnd:          xrand.New(xrand.WithSeed(1), xrand.WithLock()),
	}
	t.Cleanup(func() {
		require.NoError(t, balancer.Close(ctx))
		require.NoError(t, pool.RemoveRef(ctx))
	})

	endpoints := []endpoint.Endpoint{
		endpoint.New("node-1", endpoint.WithID(1)),
		endpoint.New("node-2", endpoint.WithID(2)),
		endpoint.New("node-3", endpoint.WithID(3)),
		endpoint.New("node-4", endpoint.WithID(4)),
	}
	balancer.applyDiscoveredEndpoints(
		ctx, endpoints, strategy.ResolvedLocation{},
	)
	require.Len(t, balancer.connections().All(), 2)

	active := connsToNodeIDMap(balancer.connections().All())
	var outside endpoint.Endpoint
	for _, candidate := range endpoints {
		if active[candidate.NodeID()] == nil {
			outside = candidate

			break
		}
	}
	require.NotNil(t, outside)

	selected, err := balancer.nextConn(endpoint.WithNodeID(ctx, outside.NodeID(), endpoint.WithFallback(false)))
	require.NoError(t, err)
	require.Equal(t, outside.Key(), selected.Endpoint().Key())
	require.Len(t, balancer.connections().All(), 3)

	balancer.applyDiscoveredEndpoints(
		ctx, endpoints, strategy.ResolvedLocation{},
	)
	require.Len(t, balancer.connections().All(), 2)
	require.NotNil(t, connsToNodeIDMap(balancer.connections().All())[outside.NodeID()])

	_, err = balancer.nextConn(endpoint.WithNodeID(ctx, 404, endpoint.WithFallback(false)))
	require.ErrorIs(t, err, ErrNoEndpoints)
}
