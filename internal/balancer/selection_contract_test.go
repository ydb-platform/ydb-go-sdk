package balancer

import (
	"testing"

	"github.com/stretchr/testify/require"

	userBalancers "github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/strategy"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
)

func TestWithNodeIDBypassesSelectionPolicies(t *testing.T) {
	connections := []conn.Conn{
		userBalancerConn(1, "preferred", state.Online),
		userBalancerConn(2, "excluded", state.Online),
	}
	balancer := userConfiguredBalancer(
		config.WithBalancer(userBalancers.PreferLocations(
			userBalancers.RandomChoice(), "preferred",
		)),
		connections,
		"",
	)

	selected, err := balancer.nextConn(userBalancers.WithNodeID(t.Context(), 2))
	require.NoError(t, err)
	require.Same(t, connections[1], selected)
}

func TestBalancerHandlesBanAndUnban(t *testing.T) {
	preferred := userBalancerConn(1, "preferred", state.Online)
	fallback := userBalancerConn(2, "fallback", state.Online)
	option := config.WithBalancer(userBalancers.PreferLocationsWithFallback(
		userBalancers.RandomChoice(), "preferred",
	))
	balancer := userConfiguredBalancer(option, []conn.Conn{preferred, fallback}, "")

	selected, err := balancer.nextConn(t.Context())
	require.NoError(t, err)
	require.Same(t, preferred, selected)

	preferred.Ban(t.Context())
	selected, err = balancer.nextConn(t.Context())
	require.NoError(t, err)
	require.Same(t, fallback, selected)

	preferred.Unban(t.Context())
	balancer.connectionsState.Store(newConnectionsStateWithBalancer(
		[]conn.Conn{preferred, fallback}, config.New(option).Balancer(), strategy.Info{}, nil,
	))
	selected, err = balancer.nextConn(t.Context())
	require.NoError(t, err)
	require.Same(t, preferred, selected)
}
