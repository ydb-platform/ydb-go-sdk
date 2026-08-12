package balancers

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/strategy"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
)

func TestPreferNearestDC(t *testing.T) {
	connections := []conn.Conn{
		&mock.Conn{AddrField: "1", LocationField: "1", StateField: state.Online},
		&mock.Conn{AddrField: "2", LocationField: "2", StateField: state.Online},
		&mock.Conn{AddrField: "3", LocationField: "2", StateField: state.Online},
	}
	policy := PreferNearestDC(RandomChoice())
	priorities := policy.Prioritize(strategy.Info{SelfLocation: "2"}, connEndpoints(connections))

	require.Len(t, priorities, len(connections))
	require.Equal(t, 2, priorityGroupCount(priorities))
	require.Equal(t, []conn.Conn{connections[1], connections[2]}, bestConnections(priorities, connections))
	require.True(t, policy.DetectsNearestDC())
}

func TestPreferLocations(t *testing.T) {
	connections := []conn.Conn{
		&mock.Conn{AddrField: "1", LocationField: "zero", StateField: state.Online},
		&mock.Conn{AddrField: "2", LocationField: "one", StateField: state.Online},
		&mock.Conn{AddrField: "3", LocationField: "two", StateField: state.Online},
	}
	policy := PreferLocations(RandomChoice(), "two", "zero")
	priorities := policy.Prioritize(strategy.Info{}, connEndpoints(connections))

	require.Len(t, priorities, len(connections))
	require.Equal(t, 2, priorityGroupCount(priorities))
	require.Equal(t, []conn.Conn{connections[0], connections[2]}, bestConnections(priorities, connections))
	require.Equal(t, "Priority{Preferences=[Locations{TWO,ZERO}]}", policy.String())
}

func TestPreferenceAliases(t *testing.T) {
	filter := func(candidate Endpoint) bool {
		return candidate.NodeID()%2 == 0
	}
	endpoints := []endpoint.Endpoint{
		endpoint.New("local", endpoint.WithID(1), endpoint.WithLocation("a")),
		endpoint.New("remote", endpoint.WithID(2), endpoint.WithLocation("b")),
	}
	info := strategy.Info{SelfLocation: "a"}

	requireSamePolicySemantics(t, info, endpoints, PreferNearestDC(RandomChoice()), PreferLocalDC(RandomChoice()))
	requireSamePolicySemantics(t,
		info, endpoints, PreferNearestDC(RandomChoice()), PreferLocalDCWithFallBack(RandomChoice()),
	)
	requireSamePolicySemantics(t,
		info, endpoints, PreferNearestDC(RandomChoice()), PreferNearestDCWithFallBack(RandomChoice()),
	)
	requireSamePolicySemantics(t, info, endpoints,
		PreferLocations(RandomChoice(), "a"),
		PreferLocationsWithFallback(RandomChoice(), "a"),
	)
	requireSamePolicySemantics(t,
		info, endpoints, Prefer(RandomChoice(), filter), PreferWithFallback(RandomChoice(), filter),
	)
}

func TestPreferLocationsRejectsEmptyList(t *testing.T) {
	require.Panics(t, func() {
		PreferLocations(RandomChoice())
	})
	require.Panics(t, func() {
		PreferLocationsWithFallback(RandomChoice())
	})
}

func TestCustomPrefer(t *testing.T) {
	connections := []conn.Conn{
		&mock.Conn{AddrField: "1", NodeIDField: 1, StateField: state.Online},
		&mock.Conn{AddrField: "2", NodeIDField: 2, StateField: state.Online},
	}
	policy := Prefer(RandomChoice(), func(candidate Endpoint) bool {
		return candidate.NodeID()%2 == 0
	})
	priorities := policy.Prioritize(strategy.Info{}, connEndpoints(connections))

	require.Len(t, priorities, len(connections))
	require.Equal(t, []conn.Conn{connections[1]}, bestConnections(priorities, connections))
	require.Contains(t, policy.String(), "Custom")
}

func TestBasicPolicies(t *testing.T) {
	require.Equal(t, RandomChoice(), RoundRobin())
	require.Equal(t, RandomChoice(), Default())
	require.True(t, SingleConn().SingleConnection())
}

func requireSamePolicySemantics(
	t *testing.T,
	info strategy.Info,
	endpoints []endpoint.Endpoint,
	expected strategy.Policy,
	actual strategy.Policy,
) {
	t.Helper()
	require.Equal(t, expected.String(), actual.String())
	require.Equal(t, expected.SingleConnection(), actual.SingleConnection())
	require.Equal(t, expected.DetectsNearestDC(), actual.DetectsNearestDC())
	require.Equal(t, expected.Prioritize(info, endpoints), actual.Prioritize(info, endpoints))
}

func bestConnections(priorities []strategy.EndpointPriority, connections []conn.Conn) []conn.Conn {
	if len(priorities) == 0 {
		return nil
	}

	minimum := priorities[0].Priority
	for _, candidate := range priorities[1:] {
		minimum = min(minimum, candidate.Priority)
	}
	best := make(map[endpoint.Key]struct{}, len(priorities))
	for _, candidate := range priorities {
		if candidate.Priority == minimum {
			best[candidate.Key] = struct{}{}
		}
	}

	result := make([]conn.Conn, 0, len(best))
	for _, connection := range connections {
		if _, ok := best[connection.Endpoint().Key()]; ok {
			result = append(result, connection)
		}
	}

	return result
}

func priorityGroupCount(priorities []strategy.EndpointPriority) int {
	groups := make(map[uint64]struct{})
	for _, candidate := range priorities {
		groups[candidate.Priority] = struct{}{}
	}

	return len(groups)
}

func connEndpoints(connections []conn.Conn) []endpoint.Endpoint {
	result := make([]endpoint.Endpoint, 0, len(connections))
	for _, connection := range connections {
		result = append(result, connection.Endpoint())
	}

	return result
}
