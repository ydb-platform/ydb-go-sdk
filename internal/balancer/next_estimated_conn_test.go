package balancer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/strategy"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
)

func TestNextEstimatedConnContinuesWithLatestSnapshot(t *testing.T) {
	replacement := &mock.Conn{
		AddrField:   "replacement:2135",
		NodeIDField: 2,
		StateField:  state.Online,
	}
	next := newConnectionsStateWithBalancer(
		[]conn.Conn{replacement}, strategy.RandomChoice(), strategy.Info{}, nil,
	)
	balancer := &Balancer{}
	staleBase := &mock.Conn{
		AddrField:   "stale:2135",
		NodeIDField: 1,
		StateField:  state.Online,
	}
	stale := &snapshotSwappingConn{
		Conn:      staleBase,
		balancer:  balancer,
		nextState: next,
	}
	previous := newConnectionsStateWithBalancer(
		[]conn.Conn{stale}, strategy.RandomChoice(), strategy.Info{}, nil,
	)
	balancer.connectionsState.Store(previous)
	stale.armed = true

	selected, failedCount := balancer.nextEstimatedConn(t.Context(), previous)

	require.Same(t, replacement, selected)
	require.Equal(t, 1, failedCount)
	require.Same(t, next, balancer.connections())
}

func TestNextEstimatedConnStopsWhenBalancerClosesDuringSelection(t *testing.T) {
	balancer := &Balancer{}
	staleBase := &mock.Conn{
		AddrField:   "stale:2135",
		NodeIDField: 1,
		StateField:  state.Online,
	}
	stale := &snapshotSwappingConn{
		Conn:     staleBase,
		balancer: balancer,
	}
	previous := newConnectionsStateWithBalancer(
		[]conn.Conn{stale}, strategy.RandomChoice(), strategy.Info{}, nil,
	)
	balancer.connectionsState.Store(previous)
	stale.armed = true

	selected, failedCount := balancer.nextEstimatedConn(t.Context(), previous)

	require.Nil(t, selected)
	require.Equal(t, 1, failedCount)
	require.Nil(t, balancer.connections())
}

func TestNextEstimatedConnStopsWhenElectionSnapshotIsEmpty(t *testing.T) {
	connections := newConnectionsStateWithEstimates(nil, nil, nil, nil)
	balancer := &Balancer{}
	balancer.connectionsState.Store(connections)

	selected, failedCount := balancer.nextEstimatedConn(t.Context(), connections)

	require.Nil(t, selected)
	require.Zero(t, failedCount)
}

func TestNextEstimatedConnStopsWhenContextIsCanceled(t *testing.T) {
	connection := &mock.Conn{AddrField: "available:2135", StateField: state.Online}
	connections := newConnectionsStateWithBalancer(
		[]conn.Conn{connection}, strategy.RandomChoice(), strategy.Info{}, nil,
	)
	balancer := &Balancer{}
	balancer.connectionsState.Store(connections)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	selected, failedCount := balancer.nextEstimatedConn(ctx, connections)

	require.Nil(t, selected)
	require.Zero(t, failedCount)
}

func TestNextConnReturnsCanceledContext(t *testing.T) {
	connection := &mock.Conn{AddrField: "available:2135", StateField: state.Online}
	balancer := &Balancer{driverConfig: config.New()}
	balancer.connectionsState.Store(newConnectionsStateWithBalancer(
		[]conn.Conn{connection}, strategy.RandomChoice(), strategy.Info{}, nil,
	))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	selected, err := balancer.nextConn(ctx)

	require.Nil(t, selected)
	require.ErrorIs(t, err, context.Canceled)
}

type snapshotSwappingConn struct {
	conn.Conn

	balancer  *Balancer
	nextState *connectionsState
	armed     bool
	swapped   bool
}

func (c *snapshotSwappingConn) State() state.State {
	if c.armed && !c.swapped {
		c.swapped = true
		c.balancer.connectionsState.Store(c.nextState)

		return state.Destroyed
	}

	return c.Conn.State()
}
