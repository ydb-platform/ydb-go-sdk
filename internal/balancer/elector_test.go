package balancer

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/policy"
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
	elector := newEndpointElector([]policy.EndpointPriority{
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
	elector := newEndpointElector([]policy.EndpointPriority{
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
	elector := newEndpointElector([]policy.EndpointPriority{
		{Key: local.Endpoint().Key()},
		{Key: remote.Endpoint().Key(), Priority: 1},
	}, connectionMap(local, remote), &electorRand{})

	local.Ban(t.Context())
	elector.Refresh()
	selected, allowBanned, ok := elector.Next()

	require.True(t, ok)
	require.False(t, allowBanned)
	require.Same(t, remote, selected)
}

func TestEndpointElectorNeverSelectsExcludedEndpoint(t *testing.T) {
	preferred := electorConnection("preferred", 1, state.Banned)
	excluded := electorConnection("excluded", 2, state.Online)
	elector := newEndpointElector([]policy.EndpointPriority{
		{Key: preferred.Endpoint().Key()},
		{Key: excluded.Endpoint().Key(), Excluded: true},
	}, connectionMap(preferred, excluded), &electorRand{})

	selected, allowBanned, ok := elector.Next()
	require.True(t, ok)
	require.True(t, allowBanned)
	require.Same(t, preferred, selected)
	require.Equal(t, 1, elector.CandidateCount())
}

func TestEndpointElectorReturnsEmptyWhenEveryEndpointIsExcluded(t *testing.T) {
	excluded := electorConnection("excluded", 1, state.Online)
	elector := newEndpointElector([]policy.EndpointPriority{
		{Key: excluded.Endpoint().Key(), Excluded: true},
	}, connectionMap(excluded), &electorRand{})

	selected, allowBanned, ok := elector.Next()
	require.False(t, ok)
	require.False(t, allowBanned)
	require.Nil(t, selected)
	require.Zero(t, elector.CandidateCount())
}

func TestEndpointElectorPessimizeAndLastResort(t *testing.T) {
	first := electorConnection("first", 1, state.Online)
	second := electorConnection("second", 2, state.Online)
	elector := newEndpointElector([]policy.EndpointPriority{
		{Key: first.Endpoint().Key()},
		{Key: second.Endpoint().Key()},
	}, connectionMap(first, second), &electorRand{})

	first.Ban(t.Context())
	require.False(t, elector.Refresh(), "exactly 50% banned must not force discovery")
	selected, allowBanned, ok := elector.Next()
	require.True(t, ok)
	require.False(t, allowBanned)
	require.Same(t, second, selected)

	second.Ban(t.Context())
	require.True(t, elector.Refresh())
	selected, allowBanned, ok = elector.Next()
	require.True(t, ok)
	require.True(t, allowBanned)
	require.Same(t, first, selected)
}

func TestEndpointElectorRefreshRestoresUnbannedConnection(t *testing.T) {
	preferred := electorConnection("preferred", 1, state.Banned)
	fallback := electorConnection("fallback", 2, state.Online)
	elector := newEndpointElector(
		[]policy.EndpointPriority{
			{Key: preferred.Endpoint().Key()},
			{Key: fallback.Endpoint().Key(), Priority: 1},
		},
		connectionMap(preferred, fallback),
		&electorRand{},
	)

	selected, _, ok := elector.Next()
	require.True(t, ok)
	require.Same(t, fallback, selected)

	preferred.Unban(t.Context())
	elector.Refresh()
	selected, _, ok = elector.Next()
	require.True(t, ok)
	require.Same(t, preferred, selected)
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
	priorities := []policy.EndpointPriority{
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
	elector.Refresh()
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
	require.False(t, nilElector.Refresh())

	empty := newEndpointElector([]policy.EndpointPriority{{}}, nil, nil)
	selected, allowBanned, ok = empty.Next()
	require.False(t, ok)
	require.Nil(t, selected)
	require.False(t, allowBanned)
	require.Zero(t, empty.CandidateCount())
	require.NotNil(t, empty.snapshot.Load())
}

func TestEndpointElectorBannedPriorityDoesNotCollideWithHealthyMaxPriority(t *testing.T) {
	healthy := electorConnection("healthy", 1, state.Online)
	banned := electorConnection("banned", 2, state.Banned)
	elector := newEndpointElector([]policy.EndpointPriority{
		{Key: healthy.Endpoint().Key(), Priority: math.MaxUint64},
		{Key: banned.Endpoint().Key()},
	}, connectionMap(healthy, banned), &electorRand{})

	selected, allowBanned, ok := elector.Next()
	require.True(t, ok)
	require.False(t, allowBanned)
	require.Same(t, healthy, selected)

	healthy.Ban(t.Context())
	elector.Refresh()
	selected, allowBanned, ok = elector.Next()
	require.True(t, ok)
	require.True(t, allowBanned)
	require.Contains(t, []conn.Conn{healthy, banned}, selected)
	require.NotNil(t, selected)
}

func TestEndpointElectorForcesDiscoveryOnceAfterMostRecordsAreBanned(t *testing.T) {
	first := electorConnection("first", 1, state.Online)
	second := electorConnection("second", 2, state.Online)
	third := electorConnection("third", 3, state.Online)
	fallback := electorConnection("fallback", 4, state.Online)
	elector := newEndpointElector([]policy.EndpointPriority{
		{Key: first.Endpoint().Key()},
		{Key: second.Endpoint().Key(), Priority: 1},
		{Key: third.Endpoint().Key(), Priority: 1},
		{Key: fallback.Endpoint().Key(), Priority: 1},
	}, connectionMap(first, second, third, fallback), &electorRand{})

	first.Ban(t.Context())
	require.False(t, elector.Refresh(), "one unavailable best-bucket endpoint is only 25% of all records")
	second.Ban(t.Context())
	require.False(t, elector.Refresh(), "exactly 50% banned must not force discovery")
	third.Ban(t.Context())
	require.True(t, elector.Refresh(), "more than 50% banned must force discovery")
	require.False(t, elector.Refresh(), "remaining above the threshold must not force discovery repeatedly")

	first.Unban(t.Context())
	second.Unban(t.Context())
	elector.Refresh()
	first.Ban(t.Context())
	require.False(t, elector.Refresh())
	second.Ban(t.Context())
	require.True(t, elector.Refresh(), "crossing the threshold again must force a new discovery")
}

func TestEndpointElectorPessimizationThresholdIgnoresExcludedEndpoints(t *testing.T) {
	first := electorConnection("first", 1, state.Online)
	second := electorConnection("second", 2, state.Online)
	excluded := electorConnection("excluded", 3, state.Banned)
	elector := newEndpointElector([]policy.EndpointPriority{
		{Key: first.Endpoint().Key()},
		{Key: second.Endpoint().Key()},
		{Key: excluded.Endpoint().Key(), Excluded: true},
	}, connectionMap(first, second, excluded), &electorRand{})

	first.Ban(t.Context())
	require.False(t, elector.Refresh(), "exactly half of eligible endpoints are banned")
	second.Ban(t.Context())
	require.True(t, elector.Refresh(), "all eligible endpoints are banned")
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
