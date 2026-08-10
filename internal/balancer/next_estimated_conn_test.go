package balancer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/strategy"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
)

func TestNextEstimatedConnLazilyCreatesConnection(t *testing.T) {
	ctx := t.Context()
	cfg := config.New()
	pool := conn.NewPool(ctx, cfg)
	candidate := endpoint.New("lazy:2135", endpoint.WithID(42))
	estimates := strategy.RandomChoice().Estimate(strategy.Info{}, []endpoint.Endpoint{candidate})
	connections := newConnectionsStateWithEstimates(
		nil, []endpoint.Endpoint{candidate}, estimates, endpointKeySet([]endpoint.Endpoint{candidate}), nil, nil,
	)
	balancer := &Balancer{
		driverConfig: cfg,
		pool:         pool,
	}
	balancer.connectionsState.Store(connections)
	t.Cleanup(func() {
		balancer.releaseStateConns(ctx, balancer.connections())
		require.NoError(t, pool.RemoveRef(ctx))
	})

	selected, failedCount := balancer.nextEstimatedConn(ctx, connections)

	require.NotNil(t, selected)
	require.Equal(t, candidate.Key(), selected.Endpoint().Key())
	require.Zero(t, failedCount)
	require.Same(t, selected, balancer.connections().Connection(candidate.Key()))
}

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
