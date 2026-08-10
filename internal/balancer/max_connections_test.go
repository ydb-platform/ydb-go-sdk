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

func TestMaxConnectionsFiltersFullSnapshotOnlyOnce(t *testing.T) {
	filter := &countingLocalFilter{}
	policy := strategy.WithMaxConnections(
		strategy.Prefer(strategy.RandomChoice(), filter, true), 2,
	)
	balancer := &Balancer{
		balancer: policy,
		rnd:      xrand.New(xrand.WithSeed(1), xrand.WithLock()),
	}
	endpoints := []endpoint.Endpoint{
		endpoint.New("local-1", endpoint.WithLocation("local")),
		endpoint.New("local-2", endpoint.WithLocation("local")),
		endpoint.New("local-3", endpoint.WithLocation("local")),
		endpoint.New("remote-1", endpoint.WithLocation("remote")),
	}

	_, selected, groups := balancer.selectDiscoveredEndpoints(
		nil, endpoints, strategy.ResolvedLocation{},
	)

	require.Equal(t, len(endpoints), filter.calls,
		"a snapshot-dependent filter must not be reevaluated on the capped endpoint set",
	)
	require.Len(t, selected, 2)
	require.Len(t, groups, 2)
	require.Equal(t, "local", selected[0].Location())
	require.Equal(t, "local", selected[1].Location())
}

type countingLocalFilter struct {
	calls int
}

func (f *countingLocalFilter) Allow(_ strategy.Info, candidate endpoint.Info) bool {
	f.calls++

	return candidate.Location() == "local"
}

func (*countingLocalFilter) String() string {
	return "CountingLocal"
}
