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
// Banned connections are not kept in the sticky set so their slots can be reused.
//
// When filter is set, preferred (filter-matching) endpoints are selected first.
// Without AllowFallback, only preferred endpoints enter the active set — matching
// PreferNearestDC / PreferLocations routing semantics and avoiding a sticky set
// that permanently excludes the local DC.
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
		if _, ok := kept[e.Key()]; ok {
			continue
		}
		fill = append(fill, e)
	}

	if rnd == nil {
		rnd = xrand.New(xrand.WithLock())
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

func connectionIndex(conns []conn.Conn, target conn.Conn) int {
	for i, cc := range conns {
		if cc == target || (cc != nil && cc.Endpoint().Key() == target.Endpoint().Key()) {
			return i
		}
	}

	return -1
}

func replacementEndpoint(
	discovered []endpoint.Endpoint,
	active []conn.Conn,
	excluded endpoint.Key,
	filter balancerConfig.Filter,
	info balancerConfig.Info,
	allowFallback bool,
) endpoint.Endpoint {
	activeKeys := connKeys(active)
	pick := func(from []endpoint.Endpoint) endpoint.Endpoint {
		for _, e := range from {
			if _, exists := activeKeys[e.Key()]; exists {
				continue
			}
			if e.Key() != excluded {
				return e
			}
		}

		return nil
	}

	if filter == nil {
		return pick(discovered)
	}

	preferred, other := partitionEndpoints(discovered, filter, info)
	if e := pick(preferred); e != nil {
		return e
	}
	if allowFallback {
		return pick(other)
	}

	return nil
}

func endpointByNodeID(endpoints []endpoint.Endpoint, nodeID uint32) endpoint.Endpoint {
	for _, e := range endpoints {
		if e.NodeID() == nodeID {
			return e
		}
	}

	return nil
}

func connKeys(conns []conn.Conn) map[endpoint.Key]struct{} {
	keys := make(map[endpoint.Key]struct{}, len(conns))
	for _, cc := range conns {
		if cc == nil {
			continue
		}
		keys[cc.Endpoint().Key()] = struct{}{}
	}

	return keys
}
