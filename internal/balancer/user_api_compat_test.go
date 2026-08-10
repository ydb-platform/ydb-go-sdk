package balancer

import (
	"testing"

	"github.com/stretchr/testify/require"

	userBalancers "github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
)

func TestUserBalancerConfigurations(t *testing.T) {
	tests := []struct {
		name         string
		option       config.Option
		selfLocation string
		connections  []conn.Conn
		allowed      map[uint32]struct{}
	}{
		{
			name:   "random choice",
			option: config.WithBalancer(userBalancers.RandomChoice()),
			connections: []conn.Conn{
				userBalancerConn(1, "a", state.Online),
				userBalancerConn(2, "b", state.Online),
			},
			allowed: nodeIDSet(1, 2),
		},
		{
			name:   "single connection",
			option: config.WithBalancer(userBalancers.SingleConn()),
			connections: []conn.Conn{
				userBalancerConn(1, "bootstrap", state.Online),
			},
			allowed: nodeIDSet(1),
		},
		{
			name: "prefer nearest dc",
			option: config.WithBalancer(userBalancers.PreferNearestDC(
				userBalancers.RandomChoice(),
			)),
			selfLocation: "a",
			connections: []conn.Conn{
				userBalancerConn(1, "a", state.Online),
				userBalancerConn(2, "b", state.Online),
			},
			allowed: nodeIDSet(1),
		},
		{
			name: "prefer nearest dc with fallback",
			option: config.WithBalancer(userBalancers.PreferNearestDCWithFallBack(
				userBalancers.RandomChoice(),
			)),
			selfLocation: "a",
			connections: []conn.Conn{
				userBalancerConn(1, "a", state.Banned),
				userBalancerConn(2, "b", state.Online),
			},
			allowed: nodeIDSet(2),
		},
		{
			name: "prefer locations",
			option: config.WithBalancer(userBalancers.PreferLocations(
				userBalancers.RandomChoice(), "a", "c",
			)),
			connections: []conn.Conn{
				userBalancerConn(1, "a", state.Online),
				userBalancerConn(2, "b", state.Online),
				userBalancerConn(3, "c", state.Online),
			},
			allowed: nodeIDSet(1, 3),
		},
		{
			name: "prefer locations with fallback",
			option: config.WithBalancer(userBalancers.PreferLocationsWithFallback(
				userBalancers.RandomChoice(), "a",
			)),
			connections: []conn.Conn{
				userBalancerConn(1, "a", state.Banned),
				userBalancerConn(2, "b", state.Online),
			},
			allowed: nodeIDSet(2),
		},
		{
			name: "custom preference",
			option: config.WithBalancer(userBalancers.Prefer(
				userBalancers.RandomChoice(),
				func(endpoint userBalancers.Endpoint) bool {
					return endpoint.NodeID()%2 == 0
				},
			)),
			connections: []conn.Conn{
				userBalancerConn(1, "a", state.Online),
				userBalancerConn(2, "b", state.Online),
				userBalancerConn(3, "c", state.Online),
			},
			allowed: nodeIDSet(2),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			b := userConfiguredBalancer(test.option, test.connections, test.selfLocation)

			for range 20 {
				selected, err := b.nextConn(t.Context())
				require.NoError(t, err)
				_, ok := test.allowed[selected.Endpoint().NodeID()]
				require.Truef(t, ok, "node %d is not allowed", selected.Endpoint().NodeID())
			}
		})
	}
}

func TestUserBalancerWithNodeIDBypassesSelectionPolicies(t *testing.T) {
	connections := []conn.Conn{
		userBalancerConn(1, "preferred", state.Online),
		userBalancerConn(2, "excluded", state.Online),
	}
	b := userConfiguredBalancer(
		config.WithBalancer(userBalancers.PreferLocations(
			userBalancers.RandomChoice(), "preferred",
		)),
		connections,
		"",
	)

	selected, err := b.nextConn(userBalancers.WithNodeID(t.Context(), 2))
	require.NoError(t, err)
	require.Same(t, connections[1], selected)
}

func TestUserBalancerHandlesBanAndUnban(t *testing.T) {
	preferred := userBalancerConn(1, "preferred", state.Online)
	fallback := userBalancerConn(2, "fallback", state.Online)
	b := userConfiguredBalancer(
		config.WithBalancer(userBalancers.PreferLocationsWithFallback(
			userBalancers.RandomChoice(), "preferred",
		)),
		[]conn.Conn{preferred, fallback},
		"",
	)

	selected, err := b.nextConn(t.Context())
	require.NoError(t, err)
	require.Same(t, preferred, selected)

	preferred.Ban(t.Context())
	selected, err = b.nextConn(t.Context())
	require.NoError(t, err)
	require.Same(t, fallback, selected)

	preferred.Unban(t.Context())
	selected, err = b.nextConn(t.Context())
	require.NoError(t, err)
	require.Same(t, preferred, selected)
}

func userConfiguredBalancer(option config.Option, connections []conn.Conn, selfLocation string) *Balancer {
	cfg := config.New(option)
	b := &Balancer{
		driverConfig:   cfg,
		balancerConfig: *cfg.Balancer(),
	}
	b.connectionsState.Store(newConnectionsState(
		connections,
		b.balancerConfig.Filter,
		balancerConfig.Info{SelfLocation: selfLocation},
		b.balancerConfig.AllowFallback,
		nil,
	))

	return b
}

func userBalancerConn(nodeID uint32, location string, connectionState state.State) conn.Conn {
	return &mock.Conn{
		AddrField:     location,
		LocationField: location,
		NodeIDField:   nodeID,
		StateField:    connectionState,
	}
}

func nodeIDSet(nodeIDs ...uint32) map[uint32]struct{} {
	result := make(map[uint32]struct{}, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		result[nodeID] = struct{}{}
	}

	return result
}
