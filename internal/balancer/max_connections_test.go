package balancer

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	userBalancers "github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
)

func TestUserMaxConnectionsConfigurations(t *testing.T) {
	tests := []struct {
		name              string
		balancer          userBalancers.Balancer
		expectedCount     int
		expectedLocations map[string]int
	}{
		{
			name:          "random choice",
			balancer:      userBalancers.WithMaxConnections(userBalancers.RandomChoice(), 3),
			expectedCount: 3,
		},
		{
			name:          "zero means unlimited",
			balancer:      userBalancers.WithMaxConnections(userBalancers.RandomChoice(), 0),
			expectedCount: 5,
		},
		{
			name: "preference without fallback",
			balancer: userBalancers.WithMaxConnections(
				userBalancers.PreferLocations(userBalancers.RandomChoice(), "local"),
				3,
			),
			expectedCount:     2,
			expectedLocations: map[string]int{"local": 2},
		},
		{
			name: "preference with fallback",
			balancer: userBalancers.WithMaxConnections(
				userBalancers.PreferLocationsWithFallback(userBalancers.RandomChoice(), "local"),
				3,
			),
			expectedCount:     3,
			expectedLocations: map[string]int{"local": 2, "remote": 1},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			b := newLimitedTestBalancer(t, test.balancer)
			b.applyDiscoveredEndpoints(t.Context(), limitedTestEndpoints(), "")

			active := b.connections().All()
			require.Len(t, active, test.expectedCount)
			if test.expectedLocations != nil {
				require.Equal(t, test.expectedLocations, connectionLocations(active))
			}
		})
	}
}

func TestMaxConnectionsSelectionIsStickyAcrossDiscovery(t *testing.T) {
	b := newLimitedTestBalancer(t,
		userBalancers.WithMaxConnections(userBalancers.RandomChoice(), 2),
	)
	endpoints := limitedTestEndpoints()

	b.applyDiscoveredEndpoints(t.Context(), endpoints, "")
	first := connectionNodeIDs(b.connections().All())
	b.applyDiscoveredEndpoints(t.Context(), endpoints, "")

	require.Equal(t, first, connectionNodeIDs(b.connections().All()))
}

func TestMaxConnectionsCompositionOrderIsPreserved(t *testing.T) {
	endpoints := []endpoint.Endpoint{
		endpoint.New("remote-1:2135", endpoint.WithID(1), endpoint.WithLocation("remote")),
		endpoint.New("remote-2:2135", endpoint.WithID(2), endpoint.WithLocation("remote")),
		endpoint.New("local-1:2135", endpoint.WithID(3), endpoint.WithLocation("local")),
		endpoint.New("local-2:2135", endpoint.WithID(4), endpoint.WithLocation("local")),
	}

	limitOutsidePreference := newLimitedTestBalancer(t,
		userBalancers.WithMaxConnections(
			userBalancers.PreferLocations(userBalancers.RandomChoice(), "local"),
			2,
		),
	)
	limitOutsidePreference.rand = noShuffleRand{}
	limitOutsidePreference.applyDiscoveredEndpoints(t.Context(), endpoints, "")
	require.Equal(t, map[string]int{"local": 2}, connectionLocations(limitOutsidePreference.connections().All()))

	preferenceOutsideLimit := newLimitedTestBalancer(t,
		userBalancers.PreferLocations(
			userBalancers.WithMaxConnections(userBalancers.RandomChoice(), 2),
			"local",
		),
	)
	preferenceOutsideLimit.rand = noShuffleRand{}
	preferenceOutsideLimit.applyDiscoveredEndpoints(t.Context(), endpoints, "")
	require.Equal(t, map[string]int{"remote": 2}, connectionLocations(preferenceOutsideLimit.connections().All()))
	_, err := preferenceOutsideLimit.nextConn(t.Context())
	require.ErrorIs(t, err, ErrNoEndpoints)
}

func TestUserWithNodeIDBypassesLimitedAndPreferencePolicies(t *testing.T) {
	b := newLimitedTestBalancer(t,
		userBalancers.WithMaxConnections(
			userBalancers.PreferLocations(userBalancers.RandomChoice(), "missing"),
			2,
		),
	)
	endpoints := limitedTestEndpoints()
	b.applyDiscoveredEndpoints(t.Context(), endpoints, "")
	require.Empty(t, b.connections().All())

	selected, err := b.nextConn(userBalancers.WithNodeID(t.Context(), endpoints[3].NodeID()))
	require.NoError(t, err)
	require.Equal(t, endpoints[3].NodeID(), selected.Endpoint().NodeID())
	require.Equal(t, state.Created, selected.State())
	require.Len(t, b.connections().All(), 1)
}

func TestLimitedBalancerBanReplacesActiveConnection(t *testing.T) {
	forced := 0
	b := newLimitedTestBalancer(t,
		userBalancers.WithMaxConnections(userBalancers.RandomChoice(), 2),
	)
	b.discoveryRepeater = &stubRepeater{forceFn: func() {
		forced++
	}}
	endpoints := limitedTestEndpoints()
	b.applyDiscoveredEndpoints(t.Context(), endpoints, "")

	banned := b.connections().All()[0]
	bannedKey := banned.Endpoint().Key()
	b.handleBan(t.Context(), banned, fmt.Errorf("connection failed"))
	require.Equal(t, state.Banned, banned.State())
	require.Equal(t, 1, forced)

	b.applyDiscoveredEndpoints(t.Context(), endpoints, "")
	require.Len(t, b.connections().All(), 2)
	for _, connection := range b.connections().All() {
		require.NotEqual(t, bannedKey, connection.Endpoint().Key())
	}
}

func newLimitedTestBalancer(t *testing.T, policy userBalancers.Balancer) *Balancer {
	t.Helper()

	ctx := context.Background()
	cfg := config.New(config.WithBalancer(policy))
	pool := conn.NewPool(ctx, cfg)
	b := &Balancer{
		driverConfig: cfg,
		balancer:     cfg.Balancer(),
		pool:         pool,
		rand:         xrand.New(xrand.WithSeed(42), xrand.WithLock()),
	}
	t.Cleanup(func() {
		if !b.closed {
			require.NoError(t, b.Close(ctx))
		}
		require.NoError(t, pool.RemoveRef(ctx))
	})

	return b
}

func limitedTestEndpoints() []endpoint.Endpoint {
	return []endpoint.Endpoint{
		endpoint.New("local-1:2135", endpoint.WithID(1), endpoint.WithLocation("local")),
		endpoint.New("local-2:2135", endpoint.WithID(2), endpoint.WithLocation("local")),
		endpoint.New("remote-1:2135", endpoint.WithID(3), endpoint.WithLocation("remote")),
		endpoint.New("remote-2:2135", endpoint.WithID(4), endpoint.WithLocation("remote")),
		endpoint.New("remote-3:2135", endpoint.WithID(5), endpoint.WithLocation("remote")),
	}
}

func connectionLocations(connections []conn.Conn) map[string]int {
	result := make(map[string]int)
	for _, connection := range connections {
		result[connection.Endpoint().Location()]++
	}

	return result
}

func connectionNodeIDs(connections []conn.Conn) []uint32 {
	result := make([]uint32, 0, len(connections))
	for _, connection := range connections {
		result = append(result, connection.Endpoint().NodeID())
	}

	return result
}

type noShuffleRand struct{}

func (noShuffleRand) Int64(int64) int64 {
	return 0
}

func (noShuffleRand) Int(int) int {
	return 0
}

func (noShuffleRand) Shuffle(int, func(i, j int)) {}
