package balancer

import (
	"cmp"
	"slices"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/policy"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
)

type activeEndpointCandidate struct {
	endpoint endpoint.Endpoint
	priority policy.EndpointPriority
	sticky   bool
	usable   bool
}

type previousEndpointState struct {
	state  state.State
	sticky bool
}

// selectActiveEndpoints applies the connection limit after policy priorities
// have been calculated and before connection wrappers are acquired from the
// pool. Existing usable endpoints are preferred within the same priority
// bucket. Known unusable endpoints are selected only when no usable endpoint
// can fill the remaining slots.
func selectActiveEndpoints(
	previous, quarantine []conn.Conn,
	previousPriorities []policy.EndpointPriority,
	endpoints []endpoint.Endpoint,
	priorities []policy.EndpointPriority,
	maxConnections int, random xrand.Rand,
) ([]endpoint.Endpoint, []policy.EndpointPriority) {
	if maxConnections <= 0 {
		return endpoints, priorities
	}
	previousStates := collectPreviousEndpointStates(previous, quarantine, previousPriorities)

	candidates := make([]activeEndpointCandidate, 0, len(endpoints))
	for i, candidate := range endpoints {
		key := candidate.Key()
		priority := priorities[i]
		if priority.Excluded {
			continue
		}

		previousState, known := previousStates[key]
		candidates = append(candidates, activeEndpointCandidate{
			endpoint: candidate,
			priority: priority,
			sticky:   previousState.sticky,
			usable:   !known || isConnectionStateUsable(previousState.state, false),
		})
	}
	random.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})
	slices.SortStableFunc(candidates, compareActiveEndpointCandidates)

	selectedCount := min(maxConnections, len(candidates))
	selectedEndpoints := make([]endpoint.Endpoint, selectedCount)
	selectedPriorities := make([]policy.EndpointPriority, selectedCount)
	for i, candidate := range candidates[:selectedCount] {
		selectedEndpoints[i] = candidate.endpoint
		selectedPriorities[i] = candidate.priority
	}

	return selectedEndpoints, selectedPriorities
}

func collectPreviousEndpointStates(
	previous, quarantine []conn.Conn,
	previousPriorities []policy.EndpointPriority,
) map[endpoint.Key]previousEndpointState {
	states := make(map[endpoint.Key]previousEndpointState, len(previous)+len(quarantine))
	for _, connection := range quarantine {
		if connection != nil {
			states[connection.Endpoint().Key()] = previousEndpointState{state: connection.State()}
		}
	}
	for _, connection := range previous {
		if connection != nil {
			states[connection.Endpoint().Key()] = previousEndpointState{state: connection.State()}
		}
	}
	for _, priority := range previousPriorities {
		previousState, ok := states[priority.Key]
		if !ok || priority.Excluded || !isConnectionStateUsable(previousState.state, false) {
			continue
		}
		previousState.sticky = true
		states[priority.Key] = previousState
	}

	return states
}

func compareActiveEndpointCandidates(lhs, rhs activeEndpointCandidate) int {
	if lhs.usable != rhs.usable {
		if lhs.usable {
			return -1
		}

		return 1
	}
	if priorityComparison := cmp.Compare(lhs.priority.Priority, rhs.priority.Priority); priorityComparison != 0 {
		return priorityComparison
	}
	if lhs.sticky != rhs.sticky {
		if lhs.sticky {
			return -1
		}

		return 1
	}

	return 0
}
