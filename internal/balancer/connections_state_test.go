package balancer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/strategy"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
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

	estimates := s.Estimations()
	estimates[0].Weight = 99
	require.Equal(t, uint64(1), s.Estimations()[0].Weight)

	require.Len(t, s.Endpoints(), 2)
	activeKeys := s.ActiveKeys()
	delete(activeKeys, connections[0].Endpoint().Key())
	require.Len(t, s.ActiveKeys(), 2)
	require.Same(t, connections[0], s.Connection(connections[0].Endpoint().Key()))
	require.Equal(t, connections[0].Endpoint().Key(), s.Endpoint(connections[0].Endpoint().Key()).Key())
	require.Equal(t, 2, s.PreferredCount())

	var nilState *connectionsState
	require.Nil(t, nilState.All())
	require.Nil(t, nilState.Endpoints())
	require.Nil(t, nilState.Estimations())
	require.Nil(t, nilState.Endpoint(endpoint.Key{}))
	require.Nil(t, nilState.Connection(endpoint.Key{}))
	require.Nil(t, nilState.ActiveKeys())
	nilState.Pessimize(endpoint.Key{})
	nilState.Unpessimize(endpoint.Key{})
	require.Nil(t, cloneEndpointKeySet(nil))

	defaultState := newConnectionsStateWithBalancer(connections, nil, strategy.Info{}, nil)
	require.Equal(t, 2, defaultState.PreferredCount())
	empty := newConnectionsStateWithEstimates(nil, nil, nil, nil, nil, nil)
	require.NotNil(t, empty.ActiveKeys())
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

	require.Equal(t, 1, s.PreferredCount())
	require.Equal(t, []strategy.Estimation{
		{Key: connections[0].Endpoint().Key(), Weight: 1},
		{Key: connections[1].Endpoint().Key(), Penalty: 1, Weight: 1},
	}, s.Estimations())
	require.Zero(t, preferredConnectionCount(nil, nil))
	require.Equal(t, 1, preferredConnectionCount(
		[]strategy.Estimation{
			{Key: connections[0].Endpoint().Key(), Penalty: ^uint64(0), Weight: 1},
			{Key: connections[0].Endpoint().Key(), Penalty: ^uint64(0), Weight: 1},
			{Key: connections[1].Endpoint().Key(), Weight: 0},
		},
		map[endpoint.Key]conn.Conn{connections[0].Endpoint().Key(): connections[0]},
	))
}

func TestConnectionsStateHandlesBanAndUnban(t *testing.T) {
	preferred := &mock.Conn{
		AddrField: "preferred", NodeIDField: 1, LocationField: "preferred", StateField: state.Online,
	}
	fallback := &mock.Conn{
		AddrField: "fallback", NodeIDField: 2, LocationField: "fallback", StateField: state.Online,
	}
	estimator := strategy.Prefer(
		strategy.RandomChoice(), "preferred",
		func(_ strategy.Info, candidate endpoint.Info) bool {
			return candidate.Location() == "preferred"
		}, true,
	)
	s := newConnectionsStateWithBalancer([]conn.Conn{preferred, fallback}, estimator, strategy.Info{}, nil)

	selected, failed := s.GetConnection(t.Context())
	require.Same(t, preferred, selected)
	require.Zero(t, failed)

	preferred.Ban(t.Context())
	selected, failed = s.GetConnection(t.Context())
	require.Same(t, fallback, selected)
	require.Equal(t, 1, failed)

	preferred.Unban(t.Context())
	s.Unpessimize(preferred.Endpoint().Key())
	selected, failed = s.GetConnection(t.Context())
	require.Same(t, preferred, selected)
	require.Zero(t, failed)
}

func TestConnectionsStatePinnedNodeContract(t *testing.T) {
	connections := []conn.Conn{
		&mock.Conn{AddrField: "1", NodeIDField: 1, StateField: state.Online},
		&mock.Conn{AddrField: "2", NodeIDField: 2, StateField: state.Online},
	}
	s := newConnectionsState(connections, nil, strategy.Info{}, false, nil)

	selected, failed := s.GetConnection(endpoint.WithNodeID(t.Context(), 2))
	require.Same(t, connections[1], selected)
	require.Zero(t, failed)

	connections[1].Ban(t.Context())
	selected, failed = s.GetConnection(endpoint.WithNodeID(t.Context(), 2, endpoint.WithFallback(false)))
	require.Nil(t, selected)
	require.Zero(t, failed)
}

func TestConnectionsStateLastResort(t *testing.T) {
	connections := []conn.Conn{
		&mock.Conn{AddrField: "1", NodeIDField: 1, StateField: state.Banned},
		&mock.Conn{AddrField: "2", NodeIDField: 2, StateField: state.Banned},
	}
	s := newConnectionsState(connections, nil, strategy.Info{Rand: deterministicRand{}}, false, nil)

	selected, failed := s.GetConnection(t.Context())
	require.Same(t, connections[0], selected)
	require.Zero(t, failed)
}

func TestConnectionsStateEmptyAndCanceled(t *testing.T) {
	s := newConnectionsState(nil, nil, strategy.Info{}, false, nil)
	selected, failed := s.GetConnection(t.Context())
	require.Nil(t, selected)
	require.Zero(t, failed)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	key, connection, allowBanned, ok := s.NextEndpoint(ctx)
	require.False(t, ok)
	require.Equal(t, endpoint.Key{}, key)
	require.Nil(t, connection)
	require.False(t, allowBanned)
	selected, failed = s.GetConnection(ctx)
	require.Nil(t, selected)
	require.Zero(t, failed)
}

func TestSelectRandomConnection(t *testing.T) {
	s := newConnectionsState(nil, nil, strategy.Info{Rand: deterministicRand{}}, false, nil)

	selected, failed := s.selectRandomConnection(nil, false)
	require.Nil(t, selected)
	require.Zero(t, failed)

	good := &mock.Conn{AddrField: "good", StateField: state.Online}
	selected, failed = s.selectRandomConnection([]conn.Conn{good}, false)
	require.Same(t, good, selected)
	require.Zero(t, failed)

	banned := &mock.Conn{AddrField: "banned", StateField: state.Banned}
	selected, failed = s.selectRandomConnection([]conn.Conn{banned}, false)
	require.Nil(t, selected)
	require.Equal(t, 1, failed)
	selected, failed = s.selectRandomConnection([]conn.Conn{banned}, true)
	require.Same(t, banned, selected)
	require.Zero(t, failed)

	selected, failed = s.selectRandomConnection([]conn.Conn{banned, good}, false)
	require.Same(t, good, selected)
	require.Zero(t, failed)
}

func TestConnsToNodeIDMap(t *testing.T) {
	require.Nil(t, connsToNodeIDMap(nil))
	connections := []conn.Conn{
		&mock.Conn{NodeIDField: 0},
		&mock.Conn{NodeIDField: 10},
	}
	require.Equal(t, map[uint32]conn.Conn{0: connections[0], 10: connections[1]}, connsToNodeIDMap(connections))
}

func TestPreviousEndpoints(t *testing.T) {
	online := &mock.Conn{AddrField: "online", StateField: state.Online}
	banned := &mock.Conn{AddrField: "banned", StateField: state.Banned}
	require.Equal(t, []strategy.PreviousEndpoint{
		{Key: online.Endpoint().Key()},
		{Key: banned.Endpoint().Key(), Banned: true},
	}, previousEndpoints([]conn.Conn{nil, online, banned}))
}

func TestIsOKConnection(t *testing.T) {
	for _, goodState := range []state.State{state.Created, state.Online, state.Offline} {
		require.True(t, isOkConnection(&mock.Conn{StateField: goodState}, false))
	}
	require.False(t, isOkConnection(&mock.Conn{StateField: state.Banned}, false))
	require.True(t, isOkConnection(&mock.Conn{StateField: state.Banned}, true))
	require.False(t, isOkConnection(&mock.Conn{StateField: state.Destroyed}, true))
}

func TestDiscoveryReuseIPAndHostName(t *testing.T) {
	ctx := t.Context()
	cfg := config.New()
	discovered := mock.Endpoint{
		AddrField: "::1:123", NodeIDField: 1, OverrideHostField: "dyn-node-1.svc.cluster.local",
	}
	balancer := &Balancer{
		driverConfig: cfg,
		balancer:     cfg.Balancer(),
		pool:         conn.NewPool(ctx, cfg),
		discover: func(context.Context, *grpc.ClientConn) ([]endpoint.Endpoint, string, error) {
			copy := discovered

			return []endpoint.Endpoint{&copy}, "", nil
		},
	}
	t.Cleanup(func() { require.NoError(t, balancer.pool.RemoveRef(ctx)) })

	check := func() {
		require.NoError(t, balancer.clusterDiscoveryAttempt(ctx, nil))
		selected, _ := balancer.connections().GetConnection(ctx)
		require.Equal(t, discovered.AddrField, selected.Endpoint().Address())
		require.Equal(t, discovered.NodeIDField, selected.Endpoint().NodeID())
		require.Equal(t, discovered.OverrideHostField, selected.Endpoint().OverrideHost())
	}

	check()
	discovered.NodeIDField = 2
	check()
	discovered.OverrideHostField = "dyn-node-2.svc.cluster.local"
	check()
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

type deterministicRand struct{}

func (deterministicRand) Int64(int64) int64 { return 0 }
func (deterministicRand) Int(int) int       { return 0 }
func (deterministicRand) Shuffle(n int, swap func(i, j int)) {
	if n > 1 {
		swap(0, n-1)
	}
}
