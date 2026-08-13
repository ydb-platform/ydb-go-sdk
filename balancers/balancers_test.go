package balancers

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/policy"
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
	p := PreferNearestDC(RandomChoice())
	priorities := p.Prioritize(policy.Info{SelfLocation: "2"}, connEndpoints(connections))

	require.Len(t, priorities, len(connections))
	require.Equal(t, 1, priorityGroupCount(priorities))
	require.True(t, priorities[0].Excluded)
	require.Equal(t, []conn.Conn{connections[1], connections[2]}, bestConnections(priorities, connections))
	require.True(t, p.DetectsNearestDC())
}

func TestPreferLocations(t *testing.T) {
	connections := []conn.Conn{
		&mock.Conn{AddrField: "1", LocationField: "zero", StateField: state.Online},
		&mock.Conn{AddrField: "2", LocationField: "one", StateField: state.Online},
		&mock.Conn{AddrField: "3", LocationField: "two", StateField: state.Online},
	}
	p := PreferLocations(RandomChoice(), "two", "zero")
	priorities := p.Prioritize(policy.Info{}, connEndpoints(connections))

	require.Len(t, priorities, len(connections))
	require.Equal(t, 1, priorityGroupCount(priorities))
	require.True(t, priorities[1].Excluded)
	require.Equal(t, []conn.Conn{connections[0], connections[2]}, bestConnections(priorities, connections))
	require.Equal(t, "Priority{Preferences=[Locations{TWO,ZERO}]}", p.String())
}

func TestNestedPreferencesPreservePublicConstructorOrder(t *testing.T) {
	endpoints := []endpoint.Endpoint{
		endpoint.New("local-even", endpoint.WithID(2), endpoint.WithLocation("local")),
		endpoint.New("local-odd", endpoint.WithID(1), endpoint.WithLocation("local")),
		endpoint.New("remote-even", endpoint.WithID(4), endpoint.WithLocation("remote")),
		endpoint.New("remote-odd", endpoint.WithID(3), endpoint.WithLocation("remote")),
	}
	p := PreferNearestDCWithFallBack(PreferWithFallback(RandomChoice(), func(candidate Endpoint) bool {
		return candidate.NodeID()%2 == 0
	}))
	priorities := p.Prioritize(policy.Info{SelfLocation: "local"}, endpoints)

	require.Equal(t, []policy.EndpointPriority{
		{Key: endpoints[0].Key()},
		{Key: endpoints[1].Key(), Priority: 1},
		{Key: endpoints[2].Key(), Priority: 2},
		{Key: endpoints[3].Key(), Priority: 3},
	}, priorities)
}

func TestPreferenceAliases(t *testing.T) {
	endpoints := []endpoint.Endpoint{
		endpoint.New("local", endpoint.WithID(1), endpoint.WithLocation("a")),
		endpoint.New("remote", endpoint.WithID(2), endpoint.WithLocation("b")),
	}
	info := policy.Info{SelfLocation: "a"}

	requireSamePolicySemantics(t, info, endpoints, PreferNearestDC(RandomChoice()), PreferLocalDC(RandomChoice()))
	requireSamePolicySemantics(t,
		info, endpoints,
		PreferNearestDCWithFallBack(RandomChoice()),
		PreferLocalDCWithFallBack(RandomChoice()),
	)
}

func TestStrictAndFallbackPreferencesDiffer(t *testing.T) {
	filter := func(candidate Endpoint) bool {
		return candidate.Location() == "a"
	}
	endpoints := []endpoint.Endpoint{
		endpoint.New("preferred", endpoint.WithLocation("a")),
		endpoint.New("other", endpoint.WithLocation("b")),
	}
	tests := []struct {
		name     string
		strict   policy.Policy
		fallback policy.Policy
	}{
		{
			name:     "nearest DC",
			strict:   PreferNearestDC(RandomChoice()),
			fallback: PreferNearestDCWithFallBack(RandomChoice()),
		},
		{
			name:     "locations",
			strict:   PreferLocations(RandomChoice(), "a"),
			fallback: PreferLocationsWithFallback(RandomChoice(), "a"),
		},
		{
			name:     "custom",
			strict:   Prefer(RandomChoice(), filter),
			fallback: PreferWithFallback(RandomChoice(), filter),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			strict := test.strict.Prioritize(policy.Info{SelfLocation: "a"}, endpoints)
			fallback := test.fallback.Prioritize(policy.Info{SelfLocation: "a"}, endpoints)

			require.Equal(t, policy.EndpointPriority{Key: endpoints[1].Key(), Excluded: true}, strict[1])
			require.Equal(t, policy.EndpointPriority{Key: endpoints[1].Key(), Priority: 1}, fallback[1])
		})
	}
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
	p := Prefer(RandomChoice(), func(candidate Endpoint) bool {
		return candidate.NodeID()%2 == 0
	})
	priorities := p.Prioritize(policy.Info{}, connEndpoints(connections))

	require.Len(t, priorities, len(connections))
	require.Equal(t, []conn.Conn{connections[1]}, bestConnections(priorities, connections))
	require.Contains(t, p.String(), "Custom")
}

func TestBasicPolicies(t *testing.T) {
	require.Equal(t, RandomChoice(), RoundRobin())
	require.Equal(t, RandomChoice(), Default())
	require.True(t, SingleConn().SingleConnection())
}

func requireSamePolicySemantics(
	t *testing.T,
	info policy.Info,
	endpoints []endpoint.Endpoint,
	expected policy.Policy,
	actual policy.Policy,
) {
	t.Helper()
	require.Equal(t, expected.String(), actual.String())
	require.Equal(t, expected.SingleConnection(), actual.SingleConnection())
	require.Equal(t, expected.DetectsNearestDC(), actual.DetectsNearestDC())
	require.Equal(t, expected.Prioritize(info, endpoints), actual.Prioritize(info, endpoints))
}

func bestConnections(priorities []policy.EndpointPriority, connections []conn.Conn) []conn.Conn {
	minimum := ^uint64(0)
	for _, candidate := range priorities {
		if !candidate.Excluded {
			minimum = min(minimum, candidate.Priority)
		}
	}
	if minimum == ^uint64(0) {
		return nil
	}
	best := make(map[endpoint.Key]struct{}, len(priorities))
	for _, candidate := range priorities {
		if !candidate.Excluded && candidate.Priority == minimum {
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

func priorityGroupCount(priorities []policy.EndpointPriority) int {
	groups := make(map[uint64]struct{})
	for _, candidate := range priorities {
		if !candidate.Excluded {
			groups[candidate.Priority] = struct{}{}
		}
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
