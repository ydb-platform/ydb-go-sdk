package balancer

import (
	"testing"

	"github.com/stretchr/testify/require"

	userBalancers "github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/strategy"
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
			selectedNodeIDs := make(map[uint32]struct{}, len(test.allowed))

			for index := range len(test.allowed) {
				b.connectionsState.Load().rand = userAPITestRand{index: index}
				selected, err := b.nextConn(t.Context())
				require.NoError(t, err)
				selectedNodeIDs[selected.Endpoint().NodeID()] = struct{}{}
			}

			require.Equal(t, test.allowed, selectedNodeIDs)
		})
	}
}

func TestUserBalancerConfigDeserializationCompatibility(t *testing.T) {
	tests := []struct {
		name            string
		serialized      string
		preferred       []uint32
		fallback        []uint32
		detectNearestDC bool
		singleConn      bool
	}{
		{name: "disable", serialized: `disable`, preferred: []uint32{1, 2, 3}, singleConn: true},
		{name: "single", serialized: `single`, preferred: []uint32{1, 2, 3}, singleConn: true},
		{name: "single JSON", serialized: `{"type":"single"}`, preferred: []uint32{1, 2, 3}, singleConn: true},
		{name: "round robin", serialized: `round_robin`, preferred: []uint32{1, 2, 3}},
		{name: "round robin JSON", serialized: `{"type":"round_robin"}`, preferred: []uint32{1, 2, 3}},
		{name: "random choice", serialized: `random_choice`, preferred: []uint32{1, 2, 3}},
		{name: "random choice JSON", serialized: `{"type":"random_choice"}`, preferred: []uint32{1, 2, 3}},
		{
			name:            "legacy local DC",
			serialized:      `{"type":"random_choice","prefer":"local_dc"}`,
			preferred:       []uint32{1},
			detectNearestDC: true,
		},
		{
			name:            "nearest DC",
			serialized:      `{"type":"random_choice","prefer":"nearest_dc"}`,
			preferred:       []uint32{1},
			detectNearestDC: true,
		},
		{
			name:            "legacy local DC with fallback",
			serialized:      `{"type":"random_choice","prefer":"local_dc","fallback":true}`,
			preferred:       []uint32{1},
			fallback:        []uint32{2, 3},
			detectNearestDC: true,
		},
		{
			name:            "nearest DC with fallback",
			serialized:      `{"type":"random_choice","prefer":"nearest_dc","fallback":true}`,
			preferred:       []uint32{1},
			fallback:        []uint32{2, 3},
			detectNearestDC: true,
		},
		{
			name:       "locations",
			serialized: `{"type":"random_choice","prefer":"locations","locations":["a","c"]}`,
			preferred:  []uint32{1, 3},
		},
		{
			name:       "locations with fallback",
			serialized: `{"type":"random_choice","prefer":"locations","locations":["a","c"],"fallback":true}`,
			preferred:  []uint32{1, 3},
			fallback:   []uint32{2},
		},
		{
			name:       "unknown preference remains random choice",
			serialized: `{"type":"random_choice","prefer":"unknown"}`,
			preferred:  []uint32{1, 2, 3},
		},
	}
	connections := []conn.Conn{
		userBalancerConn(1, "a", state.Online),
		userBalancerConn(2, "b", state.Online),
		userBalancerConn(3, "c", state.Online),
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy := userBalancers.FromConfig(test.serialized)
			state := newConnectionsStateWithBalancer(
				connections,
				policy,
				strategy.Info{SelfLocation: "a"},
				nil,
			)

			require.Equal(t, test.preferred, userBalancerNodeIDs(state.prefer))
			require.Equal(t, test.fallback, userBalancerNodeIDs(state.fallback))
			require.Equal(t, test.detectNearestDC, policy.Requirements().DetectNearestDC)
			require.Equal(t, test.singleConn, policy.Requirements().SingleConn)
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

func userConfiguredBalancer(option config.Option, connections []conn.Conn, selfLocation string) *Balancer {
	cfg := config.New(option)
	b := &Balancer{
		driverConfig: cfg,
		balancer:     cfg.Balancer(),
	}
	b.connectionsState.Store(newConnectionsStateWithBalancer(
		connections,
		b.balancer,
		strategy.Info{SelfLocation: selfLocation},
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

func userBalancerNodeIDs(connections []conn.Conn) []uint32 {
	if len(connections) == 0 {
		return nil
	}

	result := make([]uint32, len(connections))
	for i, connection := range connections {
		result[i] = connection.Endpoint().NodeID()
	}

	return result
}

type userAPITestRand struct {
	index int
}

func (userAPITestRand) Int64(int64) int64 {
	return 0
}

func (r userAPITestRand) Int(max int) int {
	return r.index % max
}

func (userAPITestRand) Shuffle(int, func(int, int)) {}
