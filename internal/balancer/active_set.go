package balancer

import (
	"cmp"
	"slices"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/policy"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
)

type activeEndpointCandidate struct {
	endpoint endpoint.Endpoint
	priority policy.EndpointPriority
	sticky   bool
	usable   bool
}

// selectActiveEndpoints applies the connection limit after policy priorities
// have been calculated and before connection wrappers are acquired from the
// pool. Existing usable endpoints are preferred within the same priority
// bucket. Known unusable endpoints are selected only when no usable endpoint
// can fill the remaining slots.
func selectActiveEndpoints(
	previous *endpointElector,
	endpoints []endpoint.Endpoint,
	priorities []policy.EndpointPriority,
	maxConnections int, random xrand.Rand,
) ([]endpoint.Endpoint, []policy.EndpointPriority) {
	if maxConnections <= 0 {
		return endpoints, priorities
	}
	var previousConnections map[endpoint.Key]conn.Conn
	var previousPrimary map[endpoint.Key]struct{}
	if previous != nil {
		previousConnections = previous.connections
		previousPrimary = make(map[endpoint.Key]struct{}, len(previous.priorities))
		for _, priority := range previous.priorities {
			if !priority.Excluded {
				previousPrimary[priority.Key] = struct{}{}
			}
		}
	}

	candidates := make([]activeEndpointCandidate, 0, len(endpoints))
	for i, candidate := range endpoints {
		priority := priorities[i]
		if priority.Excluded {
			continue
		}

		key := candidate.Key()
		connection := previousConnections[key]
		usable := connection == nil || isConnectionStateUsable(connection.State(), false)
		_, wasPrimary := previousPrimary[key]
		candidates = append(candidates, activeEndpointCandidate{
			endpoint: candidate,
			priority: priority,
			sticky:   connection != nil && usable && wasPrimary,
			usable:   usable,
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
