package strategy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
)

func TestRandomChoice(t *testing.T) {
	connections := []conn.Conn{
		strategyConn(1, "local", state.Unknown),
		strategyConn(2, "local", state.Online),
	}
	balancer := RandomChoice()

	require.Equal(t, "RandomChoice", balancer.String())
	require.Equal(t, [][]endpoint.Endpoint{connectionEndpoints(connections)},
		balancer.Filter(Info{}, connectionEndpoints(connections)),
	)

	selected, failed := balancer.Next(t.Context(), NextContext{Rand: testRand{index: 1}}, connections, false)
	require.Same(t, connections[1], selected)
	require.Zero(t, failed)

	selected, failed = balancer.Next(t.Context(), NextContext{Rand: testRand{}}, connections, false)
	require.Same(t, connections[1], selected)
	require.Zero(t, failed)
}

func TestRandomChoiceNoUsableConnections(t *testing.T) {
	connections := []conn.Conn{
		strategyConn(1, "local", state.Unknown),
		strategyConn(2, "local", state.Destroyed),
	}
	balancer := RandomChoice()

	selected, failed := balancer.Next(t.Context(), NextContext{Rand: testRand{}}, connections, false)
	require.Nil(t, selected)
	require.Equal(t, len(connections), failed)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	selected, failed = balancer.Next(ctx, NextContext{Rand: testRand{}}, connections, false)
	require.Nil(t, selected)
	require.Zero(t, failed)

	selected, failed = balancer.Next(t.Context(), NextContext{Rand: testRand{}}, nil, false)
	require.Nil(t, selected)
	require.Zero(t, failed)
}

func TestRandomChoiceCanUseBannedConnectionAsLastResort(t *testing.T) {
	banned := strategyConn(1, "local", state.Banned)
	selected, failed := RandomChoice().Next(
		t.Context(), NextContext{Rand: testRand{}}, []conn.Conn{banned}, true,
	)

	require.Same(t, banned, selected)
	require.Zero(t, failed)
}

func TestSingleConn(t *testing.T) {
	balancer := SingleConn()
	connection := strategyConn(1, "local", state.Created)

	require.Equal(t, "SingleConn", balancer.String())
	require.Equal(t, [][]endpoint.Endpoint{{connection.Endpoint()}},
		balancer.Filter(Info{}, []endpoint.Endpoint{connection.Endpoint()}),
	)

	selected, failed := balancer.Next(t.Context(), NextContext{}, []conn.Conn{connection}, false)
	require.Same(t, connection, selected)
	require.Zero(t, failed)

	banned := strategyConn(2, "local", state.Banned)
	selected, failed = balancer.Next(t.Context(), NextContext{}, []conn.Conn{banned}, false)
	require.Nil(t, selected)
	require.Equal(t, 1, failed)

	selected, failed = balancer.Next(t.Context(), NextContext{}, []conn.Conn{banned}, true)
	require.Same(t, banned, selected)
	require.Zero(t, failed)

	selected, failed = balancer.Next(t.Context(), NextContext{}, nil, false)
	require.Nil(t, selected)
	require.Zero(t, failed)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	selected, failed = balancer.Next(ctx, NextContext{}, []conn.Conn{connection}, false)
	require.Nil(t, selected)
	require.Zero(t, failed)
}

func TestIsUsableStates(t *testing.T) {
	for _, connectionState := range []state.State{state.Created, state.Online, state.Offline} {
		require.True(t, isUsable(strategyConn(1, "", connectionState), false))
	}
	require.False(t, isUsable(nil, true))
	require.False(t, isUsable(strategyConn(1, "", state.Banned), false))
	require.True(t, isUsable(strategyConn(1, "", state.Banned), true))
	require.False(t, isUsable(strategyConn(1, "", state.Destroyed), true))
}

func TestPreferFilter(t *testing.T) {
	endpoints := []endpoint.Endpoint{
		strategyConn(1, "local", state.Online).Endpoint(),
		strategyConn(2, "remote", state.Online).Endpoint(),
	}
	filter := locationFilter("local")

	withoutFallback := PreferNearestDC(RandomChoice(), filter, false)
	require.Equal(t, "Prefer{Filter=Location(local),AllowFallback=false,Child=RandomChoice}",
		withoutFallback.String(),
	)
	require.Equal(t, [][]endpoint.Endpoint{{endpoints[0]}}, withoutFallback.Filter(Info{}, endpoints))

	withFallback := Prefer(SingleConn(), filter, true)
	require.Equal(t, [][]endpoint.Endpoint{{endpoints[0]}, {endpoints[1]}},
		withFallback.Filter(Info{}, endpoints),
	)

	normalized := Prefer(nil, filter, false)
	require.Equal(t, "Prefer{Filter=Location(local),AllowFallback=false,Child=RandomChoice}", normalized.String())
}

func TestPreferNext(t *testing.T) {
	preferred := strategyConn(1, "local", state.Online)
	fallback := strategyConn(2, "remote", state.Online)
	bannedPreferred := strategyConn(3, "local", state.Banned)
	nextCtx := NextContext{Rand: testRand{}}

	withoutFallback := Prefer(RandomChoice(), locationFilter("local"), false)
	selected, failed := withoutFallback.Next(
		t.Context(), nextCtx, []conn.Conn{preferred, fallback}, false,
	)
	require.Same(t, preferred, selected)
	require.Zero(t, failed)

	selected, failed = withoutFallback.Next(
		t.Context(), nextCtx, []conn.Conn{bannedPreferred, fallback}, false,
	)
	require.Nil(t, selected)
	require.Equal(t, 1, failed)

	selected, failed = withoutFallback.Next(
		t.Context(), nextCtx, []conn.Conn{bannedPreferred, fallback}, true,
	)
	require.Same(t, bannedPreferred, selected)
	require.Zero(t, failed)

	withFallback := Prefer(RandomChoice(), locationFilter("local"), true)
	selected, failed = withFallback.Next(
		t.Context(), nextCtx, []conn.Conn{bannedPreferred, fallback}, false,
	)
	require.Same(t, fallback, selected)
	require.Equal(t, 1, failed)

	selected, failed = withFallback.Next(
		t.Context(), testRandContext(1), []conn.Conn{bannedPreferred, fallback}, true,
	)
	require.Same(t, fallback, selected)
	require.Zero(t, failed)
}

func TestPartitionWithoutFilter(t *testing.T) {
	endpoints := []endpoint.Endpoint{strategyConn(1, "local", state.Online).Endpoint()}
	preferredEndpoints, fallbackEndpoints := partitionEndpoints(endpoints, nil, Info{})
	require.Equal(t, endpoints, preferredEndpoints)
	require.Nil(t, fallbackEndpoints)

	connections := []conn.Conn{strategyConn(1, "local", state.Online)}
	preferredConnections, fallbackConnections := partitionConnections(connections, nil, Info{})
	require.Equal(t, connections, preferredConnections)
	require.Nil(t, fallbackConnections)
}

func strategyConn(nodeID uint32, location string, connectionState state.State) conn.Conn {
	return &mock.Conn{
		AddrField:     location,
		LocationField: location,
		NodeIDField:   nodeID,
		StateField:    connectionState,
	}
}

func connectionEndpoints(connections []conn.Conn) []endpoint.Endpoint {
	result := make([]endpoint.Endpoint, 0, len(connections))
	for _, connection := range connections {
		result = append(result, connection.Endpoint())
	}

	return result
}

type locationFilter string

func (f locationFilter) Allow(_ Info, candidate endpoint.Info) bool {
	return candidate.Location() == string(f)
}

func (f locationFilter) String() string {
	return "Location(" + string(f) + ")"
}

type testRand struct {
	index int
}

func (testRand) Int64(int64) int64 {
	return 0
}

func (r testRand) Int(max int) int {
	return r.index % max
}

func (testRand) Shuffle(n int, swap func(i, j int)) {
	if n > 1 {
		swap(0, n-1)
	}
}

func testRandContext(index int) NextContext {
	return NextContext{Rand: testRand{index: index}}
}
