package strategy

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
)

type maxConnections struct {
	child Balancer
	limit int
}

func (m maxConnections) Filter(info Info, endpoints []endpoint.Endpoint) [][]endpoint.Endpoint {
	return m.child.Filter(info, endpoints)
}

func (m maxConnections) Select(ctx SelectContext, endpoints []endpoint.Endpoint) []endpoint.Endpoint {
	candidates := m.child.Select(ctx, endpoints)
	if m.limit <= 0 {
		return candidates
	}

	groups := m.child.Filter(ctx.Info, candidates)
	selected := make([]endpoint.Endpoint, 0, min(m.limit, len(candidates)))
	for _, group := range groups {
		remaining := m.limit - len(selected)
		if remaining == 0 {
			break
		}
		selected = append(selected, selectEndpointsFrom(ctx.Previous, group, remaining, ctx)...)
	}

	return selected
}

func (m maxConnections) Next(
	ctx context.Context,
	nextCtx NextContext,
	connections []conn.Conn,
	allowBanned bool,
) (conn.Conn, int) {
	return m.child.Next(ctx, nextCtx, connections, allowBanned)
}

func (m maxConnections) Requirements() Requirements {
	requirements := m.child.Requirements()
	requirements.Limited = m.limit > 0 || requirements.Limited

	return requirements
}

func (m maxConnections) String() string {
	return fmt.Sprintf("MaxConnections{Limit=%d,Child=%s}", m.limit, m.child.String())
}

func selectEndpointsFrom(
	previous []conn.Conn,
	candidates []endpoint.Endpoint,
	limit int,
	ctx SelectContext,
) []endpoint.Endpoint {
	if limit <= 0 || len(candidates) == 0 {
		return nil
	}

	byKey := candidateIndex(candidates)
	banned := bannedCandidateKeys(previous, byKey)

	selected := make([]endpoint.Endpoint, 0, min(limit, len(candidates)))
	selectedKeys := make(map[endpoint.Key]struct{}, cap(selected))
	for _, connection := range previous {
		if connection == nil || connection.State() == state.Banned {
			continue
		}
		key := connection.Endpoint().Key()
		candidate, ok := byKey[key]
		if !ok {
			continue
		}
		if _, ok = selectedKeys[key]; ok {
			continue
		}
		selected = append(selected, candidate)
		selectedKeys[key] = struct{}{}
		if len(selected) == limit {
			return selected
		}
	}

	fill := make([]endpoint.Endpoint, 0, len(candidates)-len(selected))
	for _, candidate := range candidates {
		key := candidate.Key()
		if _, ok := selectedKeys[key]; ok {
			continue
		}
		if _, ok := banned[key]; ok {
			continue
		}
		fill = append(fill, candidate)
	}
	ctx.Rand.Shuffle(len(fill), func(i, j int) {
		fill[i], fill[j] = fill[j], fill[i]
	})

	needed := min(limit-len(selected), len(fill))

	return append(selected, fill[:needed]...)
}

func candidateIndex(candidates []endpoint.Endpoint) map[endpoint.Key]endpoint.Endpoint {
	result := make(map[endpoint.Key]endpoint.Endpoint, len(candidates))
	for _, candidate := range candidates {
		result[candidate.Key()] = candidate
	}

	return result
}

func bannedCandidateKeys(
	previous []conn.Conn,
	candidates map[endpoint.Key]endpoint.Endpoint,
) map[endpoint.Key]struct{} {
	result := make(map[endpoint.Key]struct{})
	for _, connection := range previous {
		if connection == nil || connection.State() != state.Banned {
			continue
		}
		key := connection.Endpoint().Key()
		if _, ok := candidates[key]; ok {
			result[key] = struct{}{}
		}
	}

	return result
}
