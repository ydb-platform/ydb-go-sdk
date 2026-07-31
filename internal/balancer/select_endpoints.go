package balancer

import (
	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
)

// selectionCtx encapsulates all parameters needed for endpoint selection.
// This eliminates the need to pass 7 parameters to every function.
type selectionCtx struct {
	previous       []conn.Conn
	discovered     []endpoint.Endpoint
	maxConnections int
	filter         balancerConfig.Filter
	info           balancerConfig.Info
	allowFallback  bool
	rnd            xrand.Rand
}

// newSelectionCtx creates a selection context from individual parameters.
func newSelectionCtx(
	previous []conn.Conn,
	discovered []endpoint.Endpoint,
	maxConnections int,
	filter balancerConfig.Filter,
	info balancerConfig.Info,
	allowFallback bool,
	rnd xrand.Rand,
) *selectionCtx {
	return &selectionCtx{
		previous:       previous,
		discovered:     discovered,
		maxConnections: maxConnections,
		filter:         filter,
		info:           info,
		allowFallback:  allowFallback,
		rnd:            rnd,
	}
}

// selectEndpoints caps the discovered endpoint list using a sticky policy similar
// to ydb-python-sdk ConnectionsCache: keep currently active endpoints that are
// still present in discovery, then fill free slots with newly sampled ones.
//
// maxConnections <= 0 means unlimited (return all discovered endpoints).
// Banned connections are not kept in the sticky set and are not re-filled from
// discovery while still present as Banned in previous, so their slots can be reused.
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
	return newSelectionCtx(previous, discovered, maxConnections, filter, info, allowFallback, rnd).selectEndpoints()
}

func (c *selectionCtx) selectEndpoints() []endpoint.Endpoint {
	if c.maxConnections <= 0 || len(c.discovered) <= c.maxConnections {
		return c.discovered
	}

	if c.filter == nil {
		return c.selectFrom(c.discovered, c.maxConnections)
	}

	preferred, other := partition(c.discovered, c.filter, c.info)
	if !c.allowFallback {
		return c.selectFrom(preferred, c.maxConnections)
	}

	selected := c.selectFrom(preferred, c.maxConnections)
	if len(selected) >= c.maxConnections {
		return selected
	}

	// Fill only the remaining budget from fallback endpoints so that the total
	// never exceeds maxConnections.
	return append(selected, c.selectFrom(other, c.maxConnections-len(selected))...)
}

// selectFrom selects up to limit endpoints from candidates,
// keeping existing healthy connections and filling remaining slots randomly.
func (c *selectionCtx) selectFrom(candidates []endpoint.Endpoint, limit int) []endpoint.Endpoint {
	if limit <= 0 || len(candidates) == 0 {
		return nil
	}
	if len(candidates) <= limit {
		return candidates
	}

	// Step 1: build lookup maps
	candidateKeys := toKeySet(candidates)
	bannedKeys := bannedKeysFrom(c.previous)

	// Step 2: keep existing healthy connections that are still in candidates
	kept := c.keepExisting(candidateKeys, limit)

	// Step 3: fill remaining slots with new candidates
	fill := c.fillCandidates(candidates, kept, bannedKeys)
	c.rnd.Shuffle(len(fill), func(i, j int) {
		fill[i], fill[j] = fill[j], fill[i]
	})

	need := min(limit-len(kept), len(fill))

	return append(kept, fill[:need]...)
}

// keepExisting returns up to limit endpoints from previous that are still in candidates.
func (c *selectionCtx) keepExisting(candidateKeys map[endpoint.Key]struct{}, limit int) []endpoint.Endpoint {
	kept := make([]endpoint.Endpoint, 0, min(len(c.previous), limit))
	for _, cc := range c.previous {
		if cc == nil || cc.State() == state.Banned {
			continue
		}
		key := cc.Endpoint().Key()
		if _, ok := candidateKeys[key]; !ok {
			continue
		}
		kept = append(kept, cc.Endpoint())
		if len(kept) >= limit {
			break
		}
	}

	return kept
}

// fillCandidates returns candidates that are not kept and not banned.
func (c *selectionCtx) fillCandidates(
	candidates, kept []endpoint.Endpoint,
	bannedKeys map[endpoint.Key]struct{},
) []endpoint.Endpoint {
	keptKeys := toKeySet(kept)
	fill := make([]endpoint.Endpoint, 0, len(candidates)-len(kept))
	for _, e := range candidates {
		key := e.Key()
		if _, ok := keptKeys[key]; ok {
			continue
		}
		if _, isBanned := bannedKeys[key]; isBanned {
			continue
		}
		fill = append(fill, e)
	}

	return fill
}

// partition splits endpoints into preferred (matching filter) and other.
// This is the same logic as sortPreferConnections but operates on endpoints.
func partition(
	discovered []endpoint.Endpoint,
	filter balancerConfig.Filter,
	info balancerConfig.Info,
) (preferred, other []endpoint.Endpoint) {
	if filter == nil {
		return discovered, nil
	}

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

// Helper functions

func toKeySet(endpoints []endpoint.Endpoint) map[endpoint.Key]struct{} {
	keys := make(map[endpoint.Key]struct{}, len(endpoints))
	for _, e := range endpoints {
		keys[e.Key()] = struct{}{}
	}

	return keys
}

func bannedKeysFrom(conns []conn.Conn) map[endpoint.Key]struct{} {
	banned := make(map[endpoint.Key]struct{})
	for _, cc := range conns {
		if cc != nil && cc.State() == state.Banned {
			banned[cc.Endpoint().Key()] = struct{}{}
		}
	}

	return banned
}
