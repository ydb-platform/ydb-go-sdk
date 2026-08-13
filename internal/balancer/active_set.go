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

// selectActiveEndpoints applies the connection limit after policy priorities
// have been calculated and before connection wrappers are acquired from the
// pool. Existing usable endpoints are preferred within the same priority
// bucket. Known unusable endpoints are selected only when no usable endpoint
// can fill the remaining slots.
func selectActiveEndpoints(
	previous, quarantine []conn.Conn,
	endpoints []endpoint.Endpoint,
	priorities []policy.EndpointPriority,
	maxConnections int, random xrand.Rand,
) ([]endpoint.Endpoint, []policy.EndpointPriority) {
	if maxConnections <= 0 {
		return endpoints, priorities
	}
	knownState := make(map[endpoint.Key]state.State, len(previous)+len(quarantine))
	for _, connection := range quarantine {
		if connection != nil {
			knownState[connection.Endpoint().Key()] = connection.State()
		}
	}
	sticky := make(map[endpoint.Key]struct{}, len(previous))
	for _, connection := range previous {
		if connection == nil {
			continue
		}
		key := connection.Endpoint().Key()
		connectionState := connection.State()
		knownState[key] = connectionState
		if isConnectionStateUsable(connectionState, false) {
			sticky[key] = struct{}{}
		}
	}

	candidates := make([]activeEndpointCandidate, 0, len(endpoints))
	for i, candidate := range endpoints {
		key := candidate.Key()
		priority := priorities[i]
		if priority.Excluded {
			continue
		}

		connectionState, known := knownState[key]
		_, wasActive := sticky[key]
		candidates = append(candidates, activeEndpointCandidate{
			endpoint: candidate,
			priority: priority,
			sticky:   wasActive,
			usable:   !known || isConnectionStateUsable(connectionState, false),
		})
	}
	slices.SortStableFunc(candidates, func(lhs, rhs activeEndpointCandidate) int {
		return cmp.Compare(lhs.priority.Priority, rhs.priority.Priority)
	})

	selected := make([]activeEndpointCandidate, 0, min(maxConnections, len(candidates)))
	selected = appendActiveEndpointCandidates(selected, candidates, maxConnections, true, random)
	selected = appendActiveEndpointCandidates(selected, candidates, maxConnections, false, random)

	selectedEndpoints := make([]endpoint.Endpoint, len(selected))
	selectedPriorities := make([]policy.EndpointPriority, len(selected))
	for i, candidate := range selected {
		selectedEndpoints[i] = candidate.endpoint
		selectedPriorities[i] = candidate.priority
	}

	return selectedEndpoints, selectedPriorities
}

func appendActiveEndpointCandidates(
	selected, candidates []activeEndpointCandidate,
	maxConnections int,
	usable bool,
	random xrand.Rand,
) []activeEndpointCandidate {
	for begin := 0; begin < len(candidates) && len(selected) < maxConnections; {
		end := begin + 1
		for end < len(candidates) && candidates[end].priority.Priority == candidates[begin].priority.Priority {
			end++
		}

		sticky := make([]activeEndpointCandidate, 0, end-begin)
		others := make([]activeEndpointCandidate, 0, end-begin)
		for _, candidate := range candidates[begin:end] {
			if candidate.usable != usable {
				continue
			}
			if candidate.sticky {
				sticky = append(sticky, candidate)
			} else {
				others = append(others, candidate)
			}
		}
		random.Shuffle(len(others), func(i, j int) {
			others[i], others[j] = others[j], others[i]
		})

		remaining := maxConnections - len(selected)
		selected = append(selected, sticky[:min(remaining, len(sticky))]...)
		remaining = maxConnections - len(selected)
		selected = append(selected, others[:min(remaining, len(others))]...)
		begin = end
	}

	return selected
}
