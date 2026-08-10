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
		}, false,
	)
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
	require.Equal(t, uint32(1), preferredConnection(balancer.connections()).Endpoint().NodeID())

	second := bridgeEndpoints(endpoint.PileStateSynchronized, endpoint.PileStatePrimary)
	balancer.applyDiscoveredEndpoints(ctx, second, strategy.ResolvedLocation{})
	preferred := preferredConnection(balancer.connections())
	require.Equal(t, uint32(2), preferred.Endpoint().NodeID())
	require.Equal(t, endpoint.PileStatePrimary,
		preferred.Endpoint().Metadata().BridgePileState,
		"the connection must expose metadata from the latest discovery snapshot",
	)

	selected, err := balancer.nextConn(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(2), selected.Endpoint().NodeID())
}

func preferredConnection(connections *connectionsState) conn.Conn {
	estimates := connections.Estimations()
	if len(estimates) == 0 {
		return nil
	}
	minimum := estimates[0]
	for _, estimation := range estimates[1:] {
		if estimation.Penalty < minimum.Penalty {
			minimum = estimation
		}
	}

	return connections.Connection(minimum.Key)
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
