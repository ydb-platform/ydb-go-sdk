package balancer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/strategy"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
)

func TestEstimatorUsesFreshDiscoveryMetadataBeforePoolGet(t *testing.T) {
	ctx := context.Background()
	cfg := config.New()
	pool := conn.NewPool(ctx, cfg)
	policy := strategy.Prefer(
		strategy.RandomChoice(), "PrimaryPile",
		func(_ strategy.Info, candidate endpoint.Info) bool {
			return candidate.Metadata().BridgePileState == endpoint.PileStatePrimary
		}, true,
	)
	balancer := &Balancer{
		driverConfig: cfg,
		estimator:    policy,
		pool:         pool,
	}
	t.Cleanup(func() {
		require.NoError(t, balancer.Close(ctx))
		require.NoError(t, pool.RemoveRef(ctx))
	})

	first := bridgeEndpoints(endpoint.PileStatePrimary, endpoint.PileStateSynchronized)
	balancer.applyDiscoveredEndpoints(ctx, first, "")
	reused := balancer.connections().connByKey[first[1].Key()]
	require.NotNil(t, reused)

	second := bridgeEndpoints(endpoint.PileStateSynchronized, endpoint.PileStatePrimary)
	balancer.applyDiscoveredEndpoints(ctx, second, "")

	selected, err := balancer.nextConn(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(2), selected.Endpoint().NodeID())
	require.Same(t, reused, selected, "the pool must reuse the existing connection wrapper")
	require.Equal(t, endpoint.PileStateSynchronized, selected.Endpoint().Metadata().BridgePileState,
		"selection must use fresh discovery metadata rather than metadata retained by the pooled connection",
	)
}

func bridgeEndpoints(first, second endpoint.PileState) []endpoint.Endpoint {
	return []endpoint.Endpoint{
		endpoint.New("node-1", endpoint.WithID(1), endpoint.WithMetadata(endpoint.Metadata{
			BridgePileState: first,
		})),
		endpoint.New("node-2", endpoint.WithID(2), endpoint.WithMetadata(endpoint.Metadata{
			BridgePileState: second,
		})),
	}
}
