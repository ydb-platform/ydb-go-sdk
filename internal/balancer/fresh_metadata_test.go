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

func TestFilterUsesFreshDiscoveryMetadataBeforePoolGet(t *testing.T) {
	ctx := context.Background()
	cfg := config.New()
	pool := conn.NewPool(ctx, cfg)
	policy := strategy.Prefer(strategy.RandomChoice(), bridgePrimaryPileFilter{}, false)
	balancer := &Balancer{
		driverConfig: cfg,
		balancer:     policy,
		pool:         pool,
	}
	t.Cleanup(func() {
		require.NoError(t, balancer.Close(ctx))
		require.NoError(t, pool.RemoveRef(ctx))
	})

	first := bridgeEndpoints(endpoint.PileStatePrimary, endpoint.PileStateSynchronized)
	balancer.applyDiscoveredEndpoints(ctx, first, strategy.ResolvedLocation{})
	require.Equal(t, uint32(1), balancer.connections().prefer[0].Endpoint().NodeID())

	second := bridgeEndpoints(endpoint.PileStateSynchronized, endpoint.PileStatePrimary)
	balancer.applyDiscoveredEndpoints(ctx, second, strategy.ResolvedLocation{})
	require.Equal(t, uint32(2), balancer.connections().prefer[0].Endpoint().NodeID())
	require.Equal(t, endpoint.PileStateSynchronized,
		balancer.connections().prefer[0].Endpoint().Metadata().BridgePileState,
		"the pool deliberately returns the existing conn with stale endpoint metadata",
	)

	selected, err := balancer.nextConn(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(2), selected.Endpoint().NodeID())
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

type bridgePrimaryPileFilter struct{}

func (bridgePrimaryPileFilter) Allow(_ strategy.Info, candidate endpoint.Info) bool {
	return candidate.Metadata().BridgePileState == endpoint.PileStatePrimary
}

func (bridgePrimaryPileFilter) String() string {
	return "PrimaryPile"
}
