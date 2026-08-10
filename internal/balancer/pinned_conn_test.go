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

func TestTryEnsurePinnedConnReturnsCurrentConnection(t *testing.T) {
	current := &mock.Conn{
		AddrField:   "current",
		NodeIDField: 42,
		StateField:  state.Online,
	}
	balancer := &Balancer{}
	balancer.connectionsState.Store(&connectionsState{
		connByNodeID: map[uint32]conn.Conn{42: current},
		connByKey:    map[endpoint.Key]conn.Conn{current.Endpoint().Key(): current},
	})

	selected, rejected := balancer.tryEnsureEndpointConn(current.Endpoint().Key())

	require.Same(t, current, selected)
	require.Nil(t, rejected)
}

func TestTryEnsurePinnedConnReturnsNothingAfterClose(t *testing.T) {
	balancer := &Balancer{closed: true}

	selected, rejected := balancer.tryEnsureEndpointConn(endpoint.New("pinned", endpoint.WithID(42)).Key())

	require.Nil(t, selected)
	require.Nil(t, rejected)
}

func TestTryEnsurePinnedConnReturnsNothingFromClosedPool(t *testing.T) {
	ctx := t.Context()
	pool := conn.NewPool(ctx, config.New())
	require.NoError(t, pool.RemoveRef(ctx))

	balancer := &Balancer{
		pool: pool,
	}
	candidate := endpoint.New("pinned", endpoint.WithID(42))
	estimates := strategy.RandomChoice().Estimate(strategy.Info{}, []endpoint.Endpoint{candidate})
	balancer.connectionsState.Store(newConnectionsStateWithEstimates(
		nil, []endpoint.Endpoint{candidate}, estimates, endpointKeySet([]endpoint.Endpoint{candidate}), nil, nil,
	))

	selected, rejected := balancer.tryEnsureEndpointConn(candidate.Key())

	require.Nil(t, selected)
	require.Nil(t, rejected)
}

func TestEnsurePinnedConnReturnsRejectedConnectionToPool(t *testing.T) {
	ctx := t.Context()
	pool := conn.NewPool(ctx, config.New())
	t.Cleanup(func() {
		require.NoError(t, pool.RemoveRef(ctx))
	})
	candidate := endpoint.New("pinned", endpoint.WithID(42))
	pooled := pool.Get(candidate)
	pooled.Ban(ctx)
	balancer := &Balancer{
		pool: pool,
	}
	estimates := strategy.RandomChoice().Estimate(strategy.Info{}, []endpoint.Endpoint{candidate})
	balancer.connectionsState.Store(newConnectionsStateWithEstimates(
		nil, []endpoint.Endpoint{candidate}, estimates, endpointKeySet([]endpoint.Endpoint{candidate}), nil, nil,
	))

	selected := balancer.ensurePinnedConn(ctx, 42)

	require.Nil(t, selected)
	pooled.Unban(ctx)
	pool.Put(ctx, pooled)
	replacement := pool.Get(candidate)
	require.NotSame(t, pooled, replacement,
		"the rejected pin must not retain an extra pool reference",
	)
	pool.Put(ctx, replacement)
}
