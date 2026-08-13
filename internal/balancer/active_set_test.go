package balancer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/policy"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
)

func TestSelectActiveEndpointsUnlimited(t *testing.T) {
	endpoints := activeSetEndpoints(1, 2)
	priorities := []policy.EndpointPriority{
		{Key: endpoints[0].Key()},
		{Key: endpoints[1].Key(), Excluded: true},
	}

	selected, selectedPriorities := selectActiveEndpoints(
		nil, nil, nil, endpoints, priorities, 0, nil,
	)
	require.Equal(t, endpoints, selected)
	require.Equal(t, priorities, selectedPriorities)

	selected, selectedPriorities = selectActiveEndpoints(
		nil, nil, nil, endpoints, priorities, -1, nil,
	)
	require.Equal(t, endpoints, selected)
	require.Equal(t, priorities, selectedPriorities)
}

func TestSelectActiveEndpointsUsesPriorityBeforeStickiness(t *testing.T) {
	endpoints := activeSetEndpoints(1, 2, 3, 4)
	priorities := []policy.EndpointPriority{
		{Key: endpoints[0].Key()},
		{Key: endpoints[1].Key()},
		{Key: endpoints[2].Key()},
		{Key: endpoints[3].Key(), Priority: 1},
	}
	previous := []conn.Conn{
		activeSetConn(endpoints[1], state.Online),
		activeSetConn(endpoints[3], state.Online),
	}

	selected, selectedPriorities := selectActiveEndpoints(
		previous, nil, priorities, endpoints, priorities, 3, noShuffleRand{},
	)

	require.Equal(t, []endpoint.Endpoint{endpoints[1], endpoints[0], endpoints[2]}, selected)
	require.Equal(t, []policy.EndpointPriority{priorities[1], priorities[0], priorities[2]}, selectedPriorities)
}

func TestSelectActiveEndpointsFillsFromFallbackPriority(t *testing.T) {
	endpoints := activeSetEndpoints(1, 2, 3)
	priorities := []policy.EndpointPriority{
		{Key: endpoints[0].Key()},
		{Key: endpoints[1].Key(), Priority: 1},
		{Key: endpoints[2].Key(), Priority: 1},
	}

	selected, selectedPriorities := selectActiveEndpoints(
		nil, nil, nil, endpoints, priorities, 2, noShuffleRand{},
	)

	require.Equal(t, []endpoint.Endpoint{endpoints[0], endpoints[1]}, selected)
	require.Equal(t, []policy.EndpointPriority{priorities[0], priorities[1]}, selectedPriorities)
}

func TestSelectActiveEndpointsReplacesKnownUnusableConnections(t *testing.T) {
	endpoints := activeSetEndpoints(1, 2, 3, 4)
	priorities := activeSetPriorities(endpoints)
	previous := []conn.Conn{
		activeSetConn(endpoints[0], state.Banned),
		activeSetConn(endpoints[1], state.Online),
	}
	quarantine := []conn.Conn{
		activeSetConn(endpoints[2], state.Destroyed),
	}

	selected, _ := selectActiveEndpoints(
		previous, quarantine, priorities, endpoints, priorities, 2, noShuffleRand{},
	)

	require.Equal(t, []endpoint.Endpoint{endpoints[1], endpoints[3]}, selected)
}

func TestSelectActiveEndpointsUsesKnownUnusableConnectionsAsLastResort(t *testing.T) {
	endpoints := activeSetEndpoints(1, 2, 3)
	priorities := []policy.EndpointPriority{
		{Key: endpoints[0].Key()},
		{Key: endpoints[1].Key()},
		{Key: endpoints[2].Key(), Priority: 1},
	}
	previous := []conn.Conn{
		activeSetConn(endpoints[0], state.Banned),
		activeSetConn(endpoints[2], state.Online),
	}
	quarantine := []conn.Conn{
		nil,
		activeSetConn(endpoints[1], state.Unknown),
	}

	selected, _ := selectActiveEndpoints(
		previous, quarantine, priorities, endpoints, priorities, 3, noShuffleRand{},
	)

	require.Equal(t, []endpoint.Endpoint{endpoints[2], endpoints[0], endpoints[1]}, selected)
}

func TestSelectActiveEndpointsExcludesIneligibleEndpoints(t *testing.T) {
	endpoints := activeSetEndpoints(1, 2)
	priorities := []policy.EndpointPriority{
		{Key: endpoints[0].Key()},
		{Key: endpoints[1].Key(), Excluded: true},
	}
	previous := []conn.Conn{nil, activeSetConn(endpoints[0], state.Online)}

	selected, selectedPriorities := selectActiveEndpoints(
		previous, nil, priorities, endpoints, priorities, 3, noShuffleRand{},
	)

	require.Equal(t, []endpoint.Endpoint{endpoints[0]}, selected)
	require.Equal(t, []policy.EndpointPriority{priorities[0]}, selectedPriorities)
}

func TestSelectActiveEndpointsRandomizesNewCandidates(t *testing.T) {
	endpoints := activeSetEndpoints(1, 2, 3)

	selected, _ := selectActiveEndpoints(
		nil, nil, nil, endpoints, activeSetPriorities(endpoints), 2, reverseRand{},
	)

	require.Equal(t, []endpoint.Endpoint{endpoints[2], endpoints[1]}, selected)
}

func TestSelectActiveEndpointsDoesNotTreatPinnedOverflowAsSticky(t *testing.T) {
	endpoints := activeSetEndpoints(1, 2, 3)
	previous := []conn.Conn{
		activeSetConn(endpoints[1], state.Online),
		activeSetConn(endpoints[0], state.Online),
	}
	previousPriorities := []policy.EndpointPriority{
		{Key: endpoints[1].Key()},
		{Key: endpoints[0].Key(), Excluded: true},
	}

	selected, _ := selectActiveEndpoints(
		previous, nil, previousPriorities, endpoints, activeSetPriorities(endpoints), 1, noShuffleRand{},
	)

	require.Equal(t, []endpoint.Endpoint{endpoints[1]}, selected)
}

func TestCompareActiveEndpointCandidatesPlacesUsableFirst(t *testing.T) {
	require.Positive(t, compareActiveEndpointCandidates(
		activeEndpointCandidate{usable: false},
		activeEndpointCandidate{usable: true},
	))
}

func activeSetEndpoints(nodeIDs ...uint32) []endpoint.Endpoint {
	result := make([]endpoint.Endpoint, len(nodeIDs))
	for i, nodeID := range nodeIDs {
		result[i] = endpoint.New(string(rune('a'+i)), endpoint.WithID(nodeID))
	}

	return result
}

func activeSetPriorities(endpoints []endpoint.Endpoint) []policy.EndpointPriority {
	result := make([]policy.EndpointPriority, len(endpoints))
	for i, candidate := range endpoints {
		result[i] = policy.EndpointPriority{Key: candidate.Key()}
	}

	return result
}

func activeSetConn(candidate endpoint.Endpoint, connectionState state.State) conn.Conn {
	return &mock.Conn{
		AddrField:   candidate.Address(),
		NodeIDField: candidate.NodeID(),
		StateField:  connectionState,
	}
}

type noShuffleRand struct{}

func (noShuffleRand) Int64(int64) int64           { return 0 }
func (noShuffleRand) Int(int) int                 { return 0 }
func (noShuffleRand) Shuffle(int, func(int, int)) {}

type reverseRand struct{}

func (reverseRand) Int64(int64) int64 { return 0 }
func (reverseRand) Int(int) int       { return 0 }
func (reverseRand) Shuffle(n int, swap func(int, int)) {
	for i, j := 0, n-1; i < j; i, j = i+1, j-1 {
		swap(i, j)
	}
}
