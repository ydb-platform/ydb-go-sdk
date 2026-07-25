package balancer

import (
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
func selectEndpoints(
	previous []conn.Conn,
	discovered []endpoint.Endpoint,
	maxConnections int,
	rnd xrand.Rand,
) []endpoint.Endpoint {
	if maxConnections <= 0 || len(discovered) <= maxConnections {
		return discovered
	}

	byKey := make(map[endpoint.Key]endpoint.Endpoint, len(discovered))
	for _, e := range discovered {
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

	candidates := make([]endpoint.Endpoint, 0, len(discovered)-len(kept))
	for _, e := range discovered {
		if _, ok := kept[e.Key()]; ok {
			continue
		}
		candidates = append(candidates, e)
	}

	if rnd == nil {
		rnd = xrand.New(xrand.WithLock())
	}
	rnd.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})

	need := maxConnections - len(keep)
	if need > len(candidates) {
		need = len(candidates)
	}

	return append(keep, candidates[:need]...)
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
