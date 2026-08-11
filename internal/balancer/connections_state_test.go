package balancer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/strategy"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
)

func TestConnectionsStateDefensiveViews(t *testing.T) {
	connections := []conn.Conn{
		&mock.Conn{AddrField: "1", NodeIDField: 1, StateField: state.Online},
		&mock.Conn{AddrField: "2", NodeIDField: 2, StateField: state.Online},
	}
	s := newConnectionsState(connections, nil, strategy.Info{}, false, nil)

	all := s.All()
	all[0] = &mock.Conn{AddrField: "mutated"}
	require.Equal(t, "1", s.All()[0].Endpoint().Address())

	var nilState *connectionsState
	require.Nil(t, nilState.All())
}

func TestConnectionsStatePolicyGroups(t *testing.T) {
	connections := []conn.Conn{
		&mock.Conn{AddrField: "local", NodeIDField: 1, LocationField: "local", StateField: state.Online},
		&mock.Conn{AddrField: "remote", NodeIDField: 2, LocationField: "remote", StateField: state.Online},
	}
	estimator := strategy.Prefer(
		strategy.RandomChoice(), "LocalDC",
		func(info strategy.Info, candidate endpoint.Info) bool {
			return candidate.Location() == info.SelfLocation
		}, true,
	)
	s := newConnectionsStateWithBalancer(connections, estimator, strategy.Info{SelfLocation: "local"}, nil)

	preferred, unavailable := s.elector.PreferenceHealth()
	require.Equal(t, 1, preferred)
	require.Zero(t, unavailable)
	require.Equal(t, []strategy.Estimation{
		{Key: connections[0].Endpoint().Key(), Weight: 1},
		{Key: connections[1].Endpoint().Key(), Penalty: 1, Weight: 1},
	}, s.elector.estimates)
}

func TestConnectionsStatePinnedNodeContract(t *testing.T) {
	connections := []conn.Conn{
		&mock.Conn{AddrField: "1", NodeIDField: 1, StateField: state.Online},
		&mock.Conn{AddrField: "2", NodeIDField: 2, StateField: state.Online},
	}
	s := newConnectionsState(connections, nil, strategy.Info{}, false, nil)

	require.Same(t, connections[1], s.preferConnection(endpoint.WithNodeID(t.Context(), 2)))
	connections[1].Ban(t.Context())
	require.Nil(t, s.preferConnection(endpoint.WithNodeID(t.Context(), 2)))
}

func TestConnectionsStateLastResort(t *testing.T) {
	connections := []conn.Conn{
		&mock.Conn{AddrField: "1", NodeIDField: 1, StateField: state.Banned},
		&mock.Conn{AddrField: "2", NodeIDField: 2, StateField: state.Banned},
	}
	s := newConnectionsStateWithBalancerAndRand(
		connections, strategy.RandomChoice(), strategy.Info{}, nil, deterministicRand{},
	)

	_, selected, allowBanned, ok := s.elector.Next()
	require.True(t, ok)
	require.True(t, allowBanned)
	require.Same(t, connections[0], selected)
}

func TestConnectionsStateEmpty(t *testing.T) {
	s := newConnectionsState(nil, nil, strategy.Info{}, false, nil)
	_, connection, _, ok := s.elector.Next()
	require.False(t, ok)
	require.Nil(t, connection)
}

func TestConnsToNodeIDMap(t *testing.T) {
	require.Nil(t, connsToNodeIDMap(nil))
	connections := []conn.Conn{
		&mock.Conn{NodeIDField: 0},
		&mock.Conn{NodeIDField: 10},
	}
	require.Equal(t, map[uint32]conn.Conn{0: connections[0], 10: connections[1]}, connsToNodeIDMap(connections))
}

type filterFunc func(info strategy.Info, candidate endpoint.Info) bool

func newConnectionsState(
	connections []conn.Conn,
	filter filterFunc,
	info strategy.Info,
	allowFallback bool,
	quarantine []conn.Conn,
) *connectionsState {
	estimator := strategy.RandomChoice()
	if filter != nil {
		estimator = strategy.Prefer(estimator, "Custom", filter, allowFallback)
	}

	return newConnectionsStateWithBalancer(connections, estimator, info, quarantine)
}

func newConnectionsStateWithBalancer(
	connections []conn.Conn,
	estimator strategy.Estimator,
	info strategy.Info,
	quarantine []conn.Conn,
) *connectionsState {
	return newConnectionsStateWithBalancerAndRand(connections, estimator, info, quarantine, nil)
}

func newConnectionsStateWithBalancerAndRand(
	connections []conn.Conn,
	estimator strategy.Estimator,
	info strategy.Info,
	quarantine []conn.Conn,
	rand xrand.Rand,
) *connectionsState {
	if estimator == nil {
		estimator = strategy.RandomChoice()
	}
	endpoints := make([]endpoint.Endpoint, 0, len(connections))
	for _, connection := range connections {
		endpoints = append(endpoints, connection.Endpoint())
	}

	return newConnectionsStateWithEstimates(
		connections, estimator.Estimate(info, endpoints), quarantine, rand,
	)
}

type deterministicRand struct{}

func (deterministicRand) Int64(int64) int64 { return 0 }
func (deterministicRand) Int(int) int       { return 0 }
func (deterministicRand) Shuffle(n int, swap func(i, j int)) {
	if n > 1 {
		swap(0, n-1)
	}
}
