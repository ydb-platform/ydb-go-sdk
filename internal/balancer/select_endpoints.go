package balancer

import (
	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
)

// selectEndpoints caps the discovered endpoint list using a sticky policy similar
// to ydb-python-sdk ConnectionsCache: keep currently active endpoints that are
// still present in discovery, then fill free slots with newly sampled ones.
//
// maxConnections <= 0 means unlimited (return all discovered endpoints).
// Banned connections are not kept in the sticky set and are not re-filled from
// discovery while still present as Banned in previous, so their slots can be reused.
//
// When a discovery result is capped and filter is set, preferred
// (filter-matching) endpoints are selected first. Without AllowFallback, only
// preferred endpoints enter the limited set, avoiding a sticky set that
// permanently excludes the local DC.
func selectEndpoints(
	previous []conn.Conn,
	discovered []endpoint.Endpoint,
	maxConnections int,
	filter balancerConfig.Filter,
	info balancerConfig.Info,
	allowFallback bool,
	rnd xrand.Rand,
) []endpoint.Endpoint {
	if maxConnections <= 0 || len(discovered) <= maxConnections {
		return discovered
	}

	if filter == nil {
		return selectEndpointsFrom(previous, discovered, maxConnections, rnd)
	}

	preferred, other := partitionEndpoints(discovered, filter, info)
	if !allowFallback {
		return selectEndpointsFrom(previous, preferred, maxConnections, rnd)
	}

	selected := selectEndpointsFrom(previous, preferred, maxConnections, rnd)
	if len(selected) >= maxConnections {
		return selected
	}

	return append(selected, selectEndpointsFrom(previous, other, maxConnections-len(selected), rnd)...)
}

//nolint:funlen
func selectEndpointsFrom(
	previous []conn.Conn,
	candidates []endpoint.Endpoint,
	maxConnections int,
	rnd xrand.Rand,
) []endpoint.Endpoint {
	if maxConnections <= 0 || len(candidates) == 0 {
		return nil
	}
	if len(candidates) <= maxConnections {
		return candidates
	}

	byKey := make(map[endpoint.Key]endpoint.Endpoint, len(candidates))
	for _, e := range candidates {
		byKey[e.Key()] = e
	}

	banned := make(map[endpoint.Key]struct{})
	for _, cc := range previous {
		if cc != nil && cc.State() == state.Banned {
			banned[cc.Endpoint().Key()] = struct{}{}
		}
	}

	keep := make([]endpoint.Endpoint, 0, maxConnections)
	kept := make(map[endpoint.Key]struct{}, maxConnections)

	for _, cc := range previous {
		if cc == nil || cc.State() == state.Banned {
			continue
		}
		key := cc.Endpoint().Key()
		e, ok := byKey[key]
		if !ok {
			continue
		}
		if _, already := kept[key]; already {
			continue
		}
		keep = append(keep, e)
		kept[key] = struct{}{}
		if len(keep) >= maxConnections {
			return keep
		}
	}

	fill := make([]endpoint.Endpoint, 0, len(candidates)-len(kept))
	for _, e := range candidates {
		key := e.Key()
		if _, ok := kept[key]; ok {
			continue
		}
		if _, isBanned := banned[key]; isBanned {
			continue
		}
		fill = append(fill, e)
	}

	rnd.Shuffle(len(fill), func(i, j int) {
		fill[i], fill[j] = fill[j], fill[i]
	})

	need := min(maxConnections-len(keep), len(fill))

	return append(keep, fill[:need]...)
}

func partitionEndpoints(
	discovered []endpoint.Endpoint,
	filter balancerConfig.Filter,
	info balancerConfig.Info,
) (preferred, other []endpoint.Endpoint) {
	preferred = make([]endpoint.Endpoint, 0, len(discovered))
	other = make([]endpoint.Endpoint, 0, len(discovered))
	for _, e := range discovered {
		if filter.Allow(info, e) {
			preferred = append(preferred, e)
		} else {
			other = append(other, e)
		}
	}

	return preferred, other
}

func endpointByNodeID(endpoints []endpoint.Endpoint, nodeID uint32) endpoint.Endpoint {
	for _, e := range endpoints {
		if e.NodeID() == nodeID {
			return e
		}
	}

	return nil
}
