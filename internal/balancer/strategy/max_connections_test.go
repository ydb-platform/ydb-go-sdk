package strategy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
)

func TestMaxConnectionsSelectsStickyActiveSet(t *testing.T) {
	endpoints := maxConnectionEndpoints(1, 2, 3, 4)
	balancer := WithMaxConnections(RandomChoice(), 2)
	ctx := SelectContext{Rand: testRand{}}

	selected := balancer.Select(ctx, endpoints)
	require.Len(t, selected, 2)

	previous := []conn.Conn{
		maxConnectionConn(selected[0], state.Online),
		maxConnectionConn(selected[1], state.Online),
	}
	selectedAgain := balancer.Select(SelectContext{Previous: previous, Rand: testRand{}}, endpoints)
	require.Equal(t, selected, selectedAgain)

	previous[0].Ban(t.Context())
	selectedAfterBan := balancer.Select(SelectContext{Previous: previous, Rand: testRand{}}, endpoints)
	require.Len(t, selectedAfterBan, 2)
	require.NotContains(t, endpointKeys(selectedAfterBan), previous[0].Endpoint().Key())
}

func TestMaxConnectionsUsesChildPreference(t *testing.T) {
	endpoints := []endpoint.Endpoint{
		endpoint.New("local-1", endpoint.WithID(1), endpoint.WithLocation("local")),
		endpoint.New("remote-1", endpoint.WithID(2), endpoint.WithLocation("remote")),
		endpoint.New("local-2", endpoint.WithID(3), endpoint.WithLocation("local")),
		endpoint.New("remote-2", endpoint.WithID(4), endpoint.WithLocation("remote")),
	}
	child := Prefer(RandomChoice(), locationFilter("local"), true)
	balancer := WithMaxConnections(child, 3)

	selected := balancer.Select(SelectContext{Rand: testRand{}}, endpoints)
	require.Len(t, selected, 3)
	require.Equal(t, "local", selected[0].Location())
	require.Equal(t, "local", selected[1].Location())

	require.Equal(t, child.Filter(Info{}, selected), balancer.Filter(Info{}, selected))
	require.Equal(t, "MaxConnections{Limit=3,Child="+child.String()+"}", balancer.String())

	selectedConn, failed := balancer.Next(
		t.Context(), NextContext{Rand: testRand{}},
		[]conn.Conn{strategyConn(1, "local", state.Online)}, false,
	)
	require.NotNil(t, selectedConn)
	require.Zero(t, failed)
}

func TestMaxConnectionsNonPositiveLimitIsUnlimited(t *testing.T) {
	endpoints := maxConnectionEndpoints(1, 2, 3)

	require.Equal(t, endpoints, WithMaxConnections(RandomChoice(), 0).Select(SelectContext{}, endpoints))
	require.Equal(t, endpoints, WithMaxConnections(RandomChoice(), -1).Select(SelectContext{}, endpoints))
}

func TestMaxConnectionsPreservesChildLifecycle(t *testing.T) {
	balancer := WithMaxConnections(
		PreferNearestDC(SingleConn(), locationFilter("local"), true), 2,
	)
	plan := Compile(balancer)
	runtime := &recordingRuntime{}

	_, err := plan.Start(t.Context(), runtime)
	require.NoError(t, err)
	require.Equal(t, "configured", runtime.source)

	resolved, err := plan.ResolveLocation(
		t.Context(), nil, "discovered",
		func(_ context.Context, _ []endpoint.Endpoint) (string, error) {
			return "detected", nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, "detected", resolved.SelfLocation)
	require.True(t, resolved.NeedLocalDC)
}

func maxConnectionEndpoints(nodeIDs ...uint32) []endpoint.Endpoint {
	result := make([]endpoint.Endpoint, len(nodeIDs))
	for i, nodeID := range nodeIDs {
		result[i] = endpoint.New(string(rune('a'+i)), endpoint.WithID(nodeID))
	}

	return result
}

func maxConnectionConn(candidate endpoint.Endpoint, connectionState state.State) conn.Conn {
	return &maxConnectionStub{endpoint: candidate, connectionState: connectionState}
}

func endpointKeys(endpoints []endpoint.Endpoint) map[endpoint.Key]struct{} {
	result := make(map[endpoint.Key]struct{}, len(endpoints))
	for _, candidate := range endpoints {
		result[candidate.Key()] = struct{}{}
	}

	return result
}

type maxConnectionStub struct {
	endpoint        endpoint.Endpoint
	connectionState state.State
}

func (c *maxConnectionStub) Endpoint() endpoint.Endpoint {
	return c.endpoint
}

func (c *maxConnectionStub) State() state.State {
	return c.connectionState
}

func (c *maxConnectionStub) Ban(context.Context) {
	c.connectionState = state.Banned
}

func (c *maxConnectionStub) Unban(context.Context) {
	c.connectionState = state.Online
}

func (*maxConnectionStub) Invoke(context.Context, string, any, any, ...grpc.CallOption) error {
	return nil
}

func (*maxConnectionStub) NewStream(
	context.Context, *grpc.StreamDesc, string, ...grpc.CallOption,
) (grpc.ClientStream, error) {
	panic("not implemented")
}
