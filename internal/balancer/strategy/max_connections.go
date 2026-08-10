package strategy

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
)

type maxConnections struct {
	child Balancer
	limit int
}

// WithMaxConnections decorates a balancer with an active-connection soft limit.
// It is internal until the user-facing option and its serialization are designed.
func WithMaxConnections(child Balancer, limit int) Balancer {
	return maxConnections{
		child: normalize(child),
		limit: max(0, limit),
	}
}

func (m maxConnections) Select(ctx SelectContext, groups [][]endpoint.Endpoint) []endpoint.Endpoint {
	candidates := m.child.Select(ctx, groups)
	if m.limit == 0 {
		return candidates
	}

	candidateKeys := make(map[endpoint.Key]struct{}, len(candidates))
	for _, candidate := range candidates {
		candidateKeys[candidate.Key()] = struct{}{}
	}
	selected := make([]endpoint.Endpoint, 0, min(m.limit, len(candidates)))
	for _, group := range groups {
		selected = append(selected, selectEndpoints(
			ctx.Previous, endpointCandidates(group, candidateKeys), m.limit-len(selected), ctx.Rand,
		)...)
		if len(selected) == m.limit {
			break
		}
	}

	return selected
}

func endpointCandidates(
	group []endpoint.Endpoint,
	candidates map[endpoint.Key]struct{},
) []endpoint.Endpoint {
	result := make([]endpoint.Endpoint, 0, len(group))
	for _, candidate := range group {
		if _, ok := candidates[candidate.Key()]; ok {
			result = append(result, candidate)
		}
	}

	return result
}

func (m maxConnections) Filter(info Info, endpoints []endpoint.Endpoint) [][]endpoint.Endpoint {
	return m.child.Filter(info, endpoints)
}

func (m maxConnections) Next(
	ctx context.Context,
	nextCtx NextContext,
	connections []conn.Conn,
	allowBanned bool,
) (conn.Conn, int) {
	return m.child.Next(ctx, nextCtx, connections, allowBanned)
}

func (m maxConnections) String() string {
	return fmt.Sprintf("MaxConnections{Limit=%d,Child=%s}", m.limit, m.child.String())
}

func (m maxConnections) compile() Plan {
	plan := compile(m.child)
	plan.balancer = m

	return plan
}

func selectEndpoints(
	previous []conn.Conn,
	candidates []endpoint.Endpoint,
	limit int,
	rnd xrand.Rand,
) []endpoint.Endpoint {
	if limit <= 0 || len(candidates) == 0 {
		return nil
	}

	byKey := make(map[endpoint.Key]endpoint.Endpoint, len(candidates))
	for _, candidate := range candidates {
		byKey[candidate.Key()] = candidate
	}

	banned := bannedEndpointKeys(previous, byKey)
	if len(candidates) <= limit && len(banned) == 0 {
		return candidates
	}

	selected, selectedKeys := stickyEndpoints(previous, byKey, limit)
	if len(selected) == limit {
		return selected
	}

	remaining := remainingEndpoints(candidates, selectedKeys, banned)
	if rnd != nil {
		rnd.Shuffle(len(remaining), func(i, j int) {
			remaining[i], remaining[j] = remaining[j], remaining[i]
		})
	}

	need := min(limit-len(selected), len(remaining))

	return append(selected, remaining[:need]...)
}

func bannedEndpointKeys(
	previous []conn.Conn,
	candidates map[endpoint.Key]endpoint.Endpoint,
) map[endpoint.Key]struct{} {
	banned := make(map[endpoint.Key]struct{})
	for _, connection := range previous {
		if connection != nil && connection.State() == state.Banned {
			key := connection.Endpoint().Key()
			if _, ok := candidates[key]; ok {
				banned[key] = struct{}{}
			}
		}
	}

	return banned
}

func stickyEndpoints(
	previous []conn.Conn,
	candidates map[endpoint.Key]endpoint.Endpoint,
	limit int,
) ([]endpoint.Endpoint, map[endpoint.Key]struct{}) {
	selected := make([]endpoint.Endpoint, 0, limit)
	selectedKeys := make(map[endpoint.Key]struct{}, limit)
	for _, connection := range previous {
		if connection == nil || connection.State() == state.Banned {
			continue
		}
		key := connection.Endpoint().Key()
		candidate, ok := candidates[key]
		if _, duplicate := selectedKeys[key]; !ok || duplicate {
			continue
		}
		selected = append(selected, candidate)
		selectedKeys[key] = struct{}{}
		if len(selected) == limit {
			break
		}
	}

	return selected, selectedKeys
}

func remainingEndpoints(
	candidates []endpoint.Endpoint,
	selected map[endpoint.Key]struct{},
	banned map[endpoint.Key]struct{},
) []endpoint.Endpoint {
	remaining := make([]endpoint.Endpoint, 0, len(candidates)-len(selected))
	for _, candidate := range candidates {
		key := candidate.Key()
		if _, ok := selected[key]; ok {
			continue
		}
		if _, ok := banned[key]; ok {
			continue
		}
		remaining = append(remaining, candidate)
	}

	return remaining
}
