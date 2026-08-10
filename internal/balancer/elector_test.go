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
	first := endpoint.New("first", endpoint.WithID(1))
	second := endpoint.New("second", endpoint.WithID(2))
	rand := &electorRand{intValue: 1}
	elector := newEndpointElector([]strategy.Estimation{
		{Key: first.Key(), Weight: 1},
		{Key: second.Key(), Weight: 1},
	}, nil, nil, rand)

	key, allowBanned, ok := elector.Next()
	require.True(t, ok)
	require.False(t, allowBanned)
	require.Equal(t, second.Key(), key)
	require.True(t, elector.snapshot.Load().uniform)

	rand.int64Value = 0
	elector = newEndpointElector([]strategy.Estimation{
		{Key: first.Key(), Weight: 1},
		{Key: second.Key(), Weight: 3},
	}, nil, nil, rand)
	key, allowBanned, ok = elector.Next()
	require.True(t, ok)
	require.False(t, allowBanned)
	require.Equal(t, first.Key(), key)
	require.False(t, elector.snapshot.Load().uniform)

	rand.int64Value = 1
	key, _, ok = elector.Next()
	require.True(t, ok)
	require.Equal(t, second.Key(), key)
}

func TestEndpointElectorUsesBestPenaltyBucket(t *testing.T) {
	first := endpoint.New("first", endpoint.WithID(1))
	second := endpoint.New("second", endpoint.WithID(2))
	elector := newEndpointElector([]strategy.Estimation{
		{Key: first.Key(), Penalty: 2, Weight: 1},
		{Key: second.Key(), Penalty: 1, Weight: 1},
	}, nil, nil, &electorRand{})

	key, allowBanned, ok := elector.Next()
	require.True(t, ok)
	require.False(t, allowBanned)
	require.Equal(t, second.Key(), key)
}

func TestEndpointElectorKeepsInactiveEndpointsAsFallback(t *testing.T) {
	active := endpoint.New("active", endpoint.WithID(1))
	inactive := endpoint.New("inactive", endpoint.WithID(2))
	estimates := []strategy.Estimation{
		{Key: active.Key(), Penalty: 2, Weight: 1},
		{Key: inactive.Key(), Weight: 1},
		{Key: endpoint.New("disabled").Key(), Weight: 0},
	}
	elector := newEndpointElector(
		estimates, nil, map[endpoint.Key]struct{}{active.Key(): {}}, &electorRand{},
	)

	key, allowBanned, ok := elector.Next()
	require.True(t, ok)
	require.False(t, allowBanned)
	require.Equal(t, active.Key(), key, "resource tier must precede a better-policy inactive endpoint")

	elector.Pessimize(active.Key())
	key, allowBanned, ok = elector.Next()
	require.True(t, ok)
	require.False(t, allowBanned)
	require.Equal(t, inactive.Key(), key)

	withoutActive := newEndpointElector(estimates, nil, map[endpoint.Key]struct{}{}, &electorRand{})
	key, _, ok = withoutActive.Next()
	require.True(t, ok)
	require.Equal(t, inactive.Key(), key)
}

func TestEndpointElectorPessimizeAndUnpessimize(t *testing.T) {
	first := endpoint.New("first", endpoint.WithID(1))
	second := endpoint.New("second", endpoint.WithID(2))
	elector := newEndpointElector([]strategy.Estimation{
		{Key: first.Key(), Weight: 1},
		{Key: second.Key(), Weight: 1},
	}, nil, nil, &electorRand{})

	elector.Pessimize(first.Key())
	elector.Pessimize(first.Key())
	key, allowBanned, ok := elector.Next()
	require.True(t, ok)
	require.False(t, allowBanned)
	require.Equal(t, second.Key(), key)

	elector.Pessimize(second.Key())
	key, allowBanned, ok = elector.Next()
	require.True(t, ok)
	require.True(t, allowBanned)
	require.Equal(t, first.Key(), key)

	elector.Unpessimize(first.Key())
	elector.Unpessimize(first.Key())
	key, allowBanned, ok = elector.Next()
	require.True(t, ok)
	require.False(t, allowBanned)
	require.Equal(t, first.Key(), key)
}

func TestEndpointElectorCombinesConnectionStateAndPolicy(t *testing.T) {
	created := electorConnection("created", 1, state.Created)
	online := electorConnection("online", 2, state.Online)
	offline := electorConnection("offline", 3, state.Offline)
	banned := electorConnection("banned", 4, state.Banned)
	unknown := electorConnection("unknown", 5, state.Unknown)
	destroyed := electorConnection("destroyed", 6, state.Destroyed)
	missing := endpoint.New("missing", endpoint.WithID(7))
	connections := map[endpoint.Key]conn.Conn{}
	for _, connection := range []conn.Conn{created, online, offline, banned, unknown, destroyed} {
		connections[connection.Endpoint().Key()] = connection
	}
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
	elector := newEndpointElector(estimates, connections, nil, &electorRand{})

	key, allowBanned, ok := elector.Next()
	require.True(t, ok)
	require.False(t, allowBanned)
	require.Equal(t, missing.Key(), key)

	for _, connection := range []conn.Conn{created, online, offline, banned} {
		require.True(t, isKnownConnection(connection))
	}
	require.False(t, isKnownConnection(unknown))
	require.False(t, isKnownConnection(destroyed))
}

func TestEndpointElectorEmptyAndLargeWeights(t *testing.T) {
	var zero endpointElector
	_, _, ok := zero.Next()
	require.False(t, ok)

	empty := newEndpointElector([]strategy.Estimation{{Weight: 0}}, nil, nil, nil)
	_, _, ok = empty.Next()
	require.False(t, ok)
	require.NotNil(t, empty.snapshot.Load())

	first := endpoint.New("first", endpoint.WithID(1))
	second := endpoint.New("second", endpoint.WithID(2))
	large := newEndpointElector([]strategy.Estimation{
		{Key: first.Key(), Weight: math.MaxUint64},
		{Key: second.Key(), Weight: math.MaxUint64},
	}, nil, nil, &electorRand{intValue: 1})
	key, _, ok := large.Next()
	require.True(t, ok)
	require.Equal(t, second.Key(), key)
	require.Equal(t, int64(math.MaxInt64-1), large.snapshot.Load().totalWeight)
}

func TestSaturatingPenalty(t *testing.T) {
	require.Equal(t, uint64(3), saturatingPenalty(1, 2))
	require.Equal(t, uint64(math.MaxUint64), saturatingPenalty(math.MaxUint64, 1))
}

func electorConnection(address string, nodeID uint32, connectionState state.State) conn.Conn {
	return &mock.Conn{AddrField: address, NodeIDField: nodeID, StateField: connectionState}
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
