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

func TestEndpointElectorSelectsRandomEndpointFromBestPriority(t *testing.T) {
	first := electorConnection("first", 1, state.Online)
	second := electorConnection("second", 2, state.Online)
	connections := connectionMap(first, second)
	rand := &electorRand{intValue: 1}
	elector := newEndpointElector([]strategy.EndpointPriority{
		{Key: first.Endpoint().Key()},
		{Key: second.Endpoint().Key()},
	}, connections, rand)

	selected, allowBanned, ok := elector.Next()
	require.True(t, ok)
	require.False(t, allowBanned)
	require.Same(t, second, selected)
}

func TestEndpointElectorUsesBestHealthyPriority(t *testing.T) {
	first := electorConnection("first", 1, state.Online)
	second := electorConnection("second", 2, state.Online)
	elector := newEndpointElector([]strategy.EndpointPriority{
		{Key: first.Endpoint().Key(), Priority: 2},
		{Key: second.Endpoint().Key(), Priority: 1},
	}, connectionMap(first, second), &electorRand{})

	selected, allowBanned, ok := elector.Next()
	require.True(t, ok)
	require.False(t, allowBanned)
	require.Same(t, second, selected)
}

func TestEndpointElectorPessimizationPromotesNextPriority(t *testing.T) {
	local := electorConnection("local", 1, state.Online)
	remote := electorConnection("remote", 2, state.Online)
	elector := newEndpointElector([]strategy.EndpointPriority{
		{Key: local.Endpoint().Key()},
		{Key: remote.Endpoint().Key(), Priority: 1},
	}, connectionMap(local, remote), &electorRand{})

	local.Ban(t.Context())
	elector.Pessimize(local.Endpoint().Key())
	selected, allowBanned, ok := elector.Next()

	require.True(t, ok)
	require.False(t, allowBanned)
	require.Same(t, remote, selected)
}

func TestEndpointElectorPessimizeAndLastResort(t *testing.T) {
	first := electorConnection("first", 1, state.Online)
	second := electorConnection("second", 2, state.Online)
	elector := newEndpointElector([]strategy.EndpointPriority{
		{Key: first.Endpoint().Key()},
		{Key: second.Endpoint().Key()},
	}, connectionMap(first, second), &electorRand{})

	first.Ban(t.Context())
	elector.Pessimize(first.Endpoint().Key())
	elector.Pessimize(first.Endpoint().Key())
	require.Equal(t, 2, elector.preferredCount)
	selected, allowBanned, ok := elector.Next()
	require.True(t, ok)
	require.False(t, allowBanned)
	require.Same(t, second, selected)

	second.Ban(t.Context())
	elector.Pessimize(second.Endpoint().Key())
	selected, allowBanned, ok = elector.Next()
	require.True(t, ok)
	require.True(t, allowBanned)
	require.Same(t, first, selected)
}

func TestEndpointElectorIgnoresUnknownPessimization(t *testing.T) {
	connection := electorConnection("known", 1, state.Online)
	elector := newEndpointElector(
		[]strategy.EndpointPriority{{Key: connection.Endpoint().Key()}},
		connectionMap(connection),
		&electorRand{},
	)

	elector.Pessimize(endpoint.New("unknown").Key())

	require.Empty(t, elector.pessimized)
	require.False(t, elector.snapshot.Load().hasPessimized)
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
	priorities := []strategy.EndpointPriority{
		{Key: created.Endpoint().Key(), Priority: 1},
		{Key: online.Endpoint().Key(), Priority: 2},
		{Key: offline.Endpoint().Key(), Priority: 3},
		{Key: banned.Endpoint().Key()},
		{Key: unknown.Endpoint().Key()},
		{Key: destroyed.Endpoint().Key()},
		{Key: missing.Key()},
	}
	elector := newEndpointElector(priorities, connections, &electorRand{})
	require.Equal(t, 4, elector.CandidateCount())
	elector.Pessimize(destroyed.Endpoint().Key())
	require.Equal(t, 4, elector.CandidateCount())

	selected, allowBanned, ok := elector.Next()
	require.True(t, ok)
	require.False(t, allowBanned)
	require.Same(t, created, selected)
}

func TestEndpointElectorEmpty(t *testing.T) {
	var zero endpointElector
	selected, allowBanned, ok := zero.Next()
	require.False(t, ok)
	require.Nil(t, selected)
	require.False(t, allowBanned)
	require.Zero(t, zero.CandidateCount())
	var nilElector *endpointElector
	require.Zero(t, nilElector.CandidateCount())
	require.Zero(t, zero.preferredCount)

	empty := newEndpointElector([]strategy.EndpointPriority{{}}, nil, nil)
	selected, allowBanned, ok = empty.Next()
	require.False(t, ok)
	require.Nil(t, selected)
	require.False(t, allowBanned)
	require.Zero(t, empty.CandidateCount())
	require.NotNil(t, empty.snapshot.Load())
}

func TestEndpointElectorPessimizationUsesMaxPriority(t *testing.T) {
	healthy := electorConnection("healthy", 1, state.Online)
	banned := electorConnection("banned", 2, state.Banned)
	elector := newEndpointElector([]strategy.EndpointPriority{
		{Key: healthy.Endpoint().Key(), Priority: math.MaxUint64 - 1},
		{Key: banned.Endpoint().Key()},
	}, connectionMap(healthy, banned), &electorRand{})

	selected, allowBanned, ok := elector.Next()
	require.True(t, ok)
	require.False(t, allowBanned)
	require.Same(t, healthy, selected)

	healthy.Ban(t.Context())
	elector.Pessimize(healthy.Endpoint().Key())
	selected, allowBanned, ok = elector.Next()
	require.True(t, ok)
	require.True(t, allowBanned)
	require.Contains(t, []conn.Conn{healthy, banned}, selected)
	require.NotNil(t, selected)
}

func TestIsConnectionStateUsable(t *testing.T) {
	for _, goodState := range []state.State{state.Created, state.Online, state.Offline} {
		require.True(t, isConnectionStateUsable(goodState, false))
	}
	require.False(t, isConnectionStateUsable(state.Banned, false))
	require.True(t, isConnectionStateUsable(state.Banned, true))
	require.False(t, isConnectionStateUsable(state.Destroyed, true))
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
	intValue int
}

func (*electorRand) Int64(int64) int64 { return 0 }

func (r *electorRand) Int(maximum int) int {
	return r.intValue % maximum
}

func (*electorRand) Shuffle(int, func(i, j int)) {}
