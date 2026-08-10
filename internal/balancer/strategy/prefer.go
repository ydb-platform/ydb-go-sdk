package strategy

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
)

type prefer struct {
	child         Balancer
	filter        Filter
	allowFallback bool
}

type nearestDC struct {
	Balancer
}

func (p prefer) Select(ctx SelectContext, endpoints []endpoint.Endpoint) []endpoint.Endpoint {
	return p.child.Select(ctx, endpoints)
}

func (p prefer) Filter(info Info, endpoints []endpoint.Endpoint) [][]endpoint.Endpoint {
	preferred, fallback := partitionEndpoints(endpoints, p.filter, info)
	groups := p.child.Filter(info, preferred)
	if p.allowFallback {
		groups = append(groups, p.child.Filter(info, fallback)...)
	}

	return groups
}

func (p prefer) Next(
	ctx context.Context,
	nextCtx NextContext,
	connections []conn.Conn,
	allowBanned bool,
) (conn.Conn, int) {
	return p.child.Next(ctx, nextCtx, connections, allowBanned)
}

func (p prefer) String() string {
	return fmt.Sprintf("Prefer{Filter=%s,AllowFallback=%t,Child=%s}",
		p.filter.String(), p.allowFallback, p.child.String(),
	)
}

func partitionEndpoints(
	endpoints []endpoint.Endpoint,
	filter Filter,
	info Info,
) (preferred, fallback []endpoint.Endpoint) {
	if filter == nil {
		return endpoints, nil
	}

	preferred = make([]endpoint.Endpoint, 0, len(endpoints))
	fallback = make([]endpoint.Endpoint, 0, len(endpoints))
	for _, candidate := range endpoints {
		if filter.Allow(info, candidate) {
			preferred = append(preferred, candidate)
		} else {
			fallback = append(fallback, candidate)
		}
	}

	return preferred, fallback
}
