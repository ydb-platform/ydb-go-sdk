package balancer

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/strategy"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
)

func TestEndpointElectorUniformAndWeightedSelection(t *testing.T) {
	first := electorConnection("first", 1, state.Online)
	second := electorConnection("second", 2, state.Online)
	connections := connectionMap(first, second)
	rand := &electorRand{intValue: 1}
	elector := newEndpointElector([]strategy.Estimation{
		{Key: first.Endpoint().Key(), Weight: 1},
		{Key: second.Endpoint().Key(), Weight: 1},
	}, connections, rand)

	key, selected, allowBanned, ok := elector.Next()
	require.True(t, ok)
	require.False(t, allowBanned)
	require.Equal(t, second.Endpoint().Key(), key)
	require.Same(t, second, selected)
	require.True(t, elector.snapshot.Load().uniform)

	rand.int64Value = 0
	elector = newEndpointElector([]strategy.Estimation{
		{Key: first.Endpoint().Key(), Weight: 1},
		{Key: second.Endpoint().Key(), Weight: 3},
	}, connections, rand)
	key, selected, allowBanned, ok = elector.Next()
	require.True(t, ok)
	require.False(t, allowBanned)
	require.Equal(t, first.Endpoint().Key(), key)
	require.Same(t, first, selected)
	require.False(t, elector.snapshot.Load().uniform)

	rand.int64Value = 1
	key, selected, _, ok = elector.Next()
	require.True(t, ok)
	require.Equal(t, second.Endpoint().Key(), key)
	require.Same(t, second, selected)
}

func TestEndpointElectorUsesBestHealthyPenaltyBucket(t *testing.T) {
	first := electorConnection("first", 1, state.Online)
	second := electorConnection("second", 2, state.Online)
	elector := newEndpointElector([]strategy.Estimation{
		{Key: first.Endpoint().Key(), Penalty: 2, Weight: 1},
		{Key: second.Endpoint().Key(), Penalty: 1, Weight: 1},
	}, connectionMap(first, second), &electorRand{})

	key, selected, allowBanned, ok := elector.Next()
	require.True(t, ok)
	require.False(t, allowBanned)
	require.Equal(t, second.Endpoint().Key(), key)
	require.Same(t, second, selected)
}

func TestEndpointElectorPessimizeAndLastResort(t *testing.T) {
	first := electorConnection("first", 1, state.Online)
	second := electorConnection("second", 2, state.Online)
	elector := newEndpointElector([]strategy.Estimation{
		{Key: first.Endpoint().Key(), Weight: 1},
		{Key: second.Endpoint().Key(), Weight: 1},
	}, connectionMap(first, second), &electorRand{})

	elector.Pessimize(first.Endpoint().Key())
	elector.Pessimize(first.Endpoint().Key())
	preferred, unavailable := elector.PreferenceHealth()
	require.Equal(t, 2, preferred)
	require.Equal(t, 1, unavailable)
	key, selected, allowBanned, ok := elector.Next()
	require.True(t, ok)
	require.False(t, allowBanned)
	require.Equal(t, second.Endpoint().Key(), key)
	require.Same(t, second, selected)

	elector.Pessimize(second.Endpoint().Key())
	key, selected, allowBanned, ok = elector.Next()
	require.True(t, ok)
	require.True(t, allowBanned)
	require.Equal(t, first.Endpoint().Key(), key)
	require.Same(t, first, selected)
}

func TestEndpointElectorCombinesConnectionStateAndPolicy(t *testing.T) {
	created := electorConnection("created", 1, state.Created)
	online := electorConnection("online", 2, state.Online)
	offline := electorConnection("offline", 3, state.Offline)
	banned := electorConnection("banned", 4, state.Banned)
	unknown := electorConnection("unknown", 5, state.Unknown)
	destroyed := electorConnection("destroyed", 6, state.Destroyed)
	missing := endpoint.New("missing", endpoint.WithID(7))
	connections := connectionMap(created, online, offline, banned, unknown, destroyed)
	estimates := []strategy.Estimation{
		{Key: created.Endpoint().Key(), Penalty: 1, Weight: 1},
		{Key: online.Endpoint().Key(), Penalty: 2, Weight: 1},
		{Key: offline.Endpoint().Key(), Penalty: 3, Weight: 1},
		{Key: banned.Endpoint().Key(), Weight: 1},
		{Key: unknown.Endpoint().Key(), Weight: 1},
		{Key: destroyed.Endpoint().Key(), Weight: 1},
		{Key: missing.Key(), Weight: 1},
		{Key: endpoint.New("zero-weight").Key(), Weight: 0},
	}
	elector := newEndpointElector(estimates, connections, &electorRand{})

	key, selected, allowBanned, ok := elector.Next()
	require.True(t, ok)
	require.False(t, allowBanned)
	require.Equal(t, created.Endpoint().Key(), key)
	require.Same(t, created, selected)
}

func TestEndpointElectorEmptyAndLargeWeights(t *testing.T) {
	var zero endpointElector
	key, selected, allowBanned, ok := zero.Next()
	require.False(t, ok)
	require.Equal(t, endpoint.Key{}, key)
	require.Nil(t, selected)
	require.False(t, allowBanned)
	require.Zero(t, zero.CandidateCount())
	var nilElector *endpointElector
	require.Zero(t, nilElector.CandidateCount())
	preferred, unavailable := zero.PreferenceHealth()
	require.Zero(t, preferred)
	require.Zero(t, unavailable)

	empty := newEndpointElector([]strategy.Estimation{{Weight: 0}}, nil, nil)
	key, selected, allowBanned, ok = empty.Next()
	require.False(t, ok)
	require.Equal(t, endpoint.Key{}, key)
	require.Nil(t, selected)
	require.False(t, allowBanned)
	require.Equal(t, 1, empty.CandidateCount())
	require.NotNil(t, empty.snapshot.Load())

	first := electorConnection("first", 1, state.Online)
	second := electorConnection("second", 2, state.Online)
	large := newEndpointElector([]strategy.Estimation{
		{Key: first.Endpoint().Key(), Weight: math.MaxUint64},
		{Key: second.Endpoint().Key(), Weight: math.MaxUint64 / 2},
	}, connectionMap(first, second), &electorRand{})
	key, selected, allowBanned, ok = large.Next()
	require.True(t, ok)
	require.False(t, allowBanned)
	require.Equal(t, first.Endpoint().Key(), key)
	require.Same(t, first, selected)
	require.False(t, large.snapshot.Load().uniform)
	require.Greater(t, large.snapshot.Load().entries[0].cumulative,
		large.snapshot.Load().totalWeight-large.snapshot.Load().entries[0].cumulative,
	)
	require.LessOrEqual(t, large.snapshot.Load().totalWeight, int64(math.MaxInt64))
	require.Empty(t, normalizeElectionWeights(nil))
}

func TestEndpointElectorKeepsHealthyMaxPenaltyAboveBannedEndpoint(t *testing.T) {
	healthy := electorConnection("healthy", 1, state.Online)
	banned := electorConnection("banned", 2, state.Banned)
	elector := newEndpointElector([]strategy.Estimation{
		{Key: healthy.Endpoint().Key(), Penalty: math.MaxUint64, Weight: 1},
		{Key: banned.Endpoint().Key(), Weight: 1},
	}, connectionMap(healthy, banned), &electorRand{})

	key, selected, allowBanned, ok := elector.Next()
	require.True(t, ok)
	require.False(t, allowBanned)
	require.Equal(t, healthy.Endpoint().Key(), key)
	require.Same(t, healthy, selected)
}

func TestIsConnectionUsable(t *testing.T) {
	require.False(t, isConnectionUsable(nil, true))
	for _, goodState := range []state.State{state.Created, state.Online, state.Offline} {
		require.True(t, isConnectionUsable(&mock.Conn{StateField: goodState}, false))
	}
	require.False(t, isConnectionUsable(&mock.Conn{StateField: state.Banned}, false))
	require.True(t, isConnectionUsable(&mock.Conn{StateField: state.Banned}, true))
	require.False(t, isConnectionUsable(&mock.Conn{StateField: state.Destroyed}, true))
}

func electorConnection(address string, nodeID uint32, connectionState state.State) conn.Conn {
	return &mock.Conn{AddrField: address, NodeIDField: nodeID, StateField: connectionState}
}

func connectionMap(connections ...conn.Conn) map[endpoint.Key]conn.Conn {
	result := make(map[endpoint.Key]conn.Conn, len(connections))
	for _, connection := range connections {
		result[connection.Endpoint().Key()] = connection
	}

	return result
}

type electorRand struct {
	intValue   int
	int64Value int64
}

func (r *electorRand) Int64(maximum int64) int64 {
	return r.int64Value % maximum
}

func (r *electorRand) Int(maximum int) int {
	return r.intValue % maximum
}

func (*electorRand) Shuffle(int, func(i, j int)) {}
