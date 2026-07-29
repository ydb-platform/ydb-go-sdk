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
		return c.selectFrom(c.discovered)
	}

	preferred, other := partition(c.discovered, c.filter, c.info)
	if !c.allowFallback {
		return c.selectFrom(preferred)
	}

	selected := c.selectFrom(preferred)
	if len(selected) >= c.maxConnections {
		return selected
	}

	return append(selected, c.selectFrom(other)...)
}

// selectFrom selects up to maxConnections endpoints from candidates,
// keeping existing healthy connections and filling remaining slots randomly.
func (c *selectionCtx) selectFrom(candidates []endpoint.Endpoint) []endpoint.Endpoint {
	if c.maxConnections <= 0 || len(candidates) == 0 {
		return nil
	}
	if len(candidates) <= c.maxConnections {
		return candidates
	}

	// Step 1: build lookup maps
	candidateKeys := toKeySet(candidates)
	bannedKeys := bannedKeysFrom(c.previous)

	// Step 2: keep existing healthy connections that are still in candidates
	kept := c.keepExisting(candidateKeys)

	// Step 3: fill remaining slots with new candidates
	fill := c.fillCandidates(candidates, kept, bannedKeys)
	c.rnd.Shuffle(len(fill), swap)

	need := min(c.maxConnections-len(kept), len(fill))
	return append(kept, fill[:need]...)
}

// keepExisting returns endpoints from previous that are still in candidates.
func (c *selectionCtx) keepExisting(candidateKeys map[endpoint.Key]struct{}) []endpoint.Endpoint {
	kept := make([]endpoint.Endpoint, 0, min(len(c.previous), c.maxConnections))
	for _, cc := range c.previous {
		if cc == nil || cc.State() == state.Banned {
			continue
		}
		key := cc.Endpoint().Key()
		if _, ok := candidateKeys[key]; !ok {
			continue
		}
		kept = append(kept, cc.Endpoint())
		if len(kept) >= c.maxConnections {
			break
		}
	}
	return kept
}

// fillCandidates returns candidates that are not kept and not banned.
func (c *selectionCtx) fillCandidates(candidates, kept []endpoint.Endpoint, bannedKeys map[endpoint.Key]struct{}) []endpoint.Endpoint {
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

// connectionIndex returns the index of target in conns, or -1 if not found.
func connectionIndex(conns []conn.Conn, target conn.Conn) int {
	for i, cc := range conns {
		if cc == target || (cc != nil && cc.Endpoint().Key() == target.Endpoint().Key()) {
			return i
		}
	}

	return -1
}

// findReplacement finds a replacement endpoint for a banned connection.
// Returns nil if no replacement is available.
func (c *selectionCtx) findReplacement(excluded endpoint.Key) endpoint.Endpoint {
	activeKeys := connKeys(c.previous)

	candidates := make([]endpoint.Endpoint, 0, len(c.discovered))
	for _, e := range c.discovered {
		if _, exists := activeKeys[e.Key()]; exists {
			continue
		}
		if e.Key() == excluded {
			continue
		}
		candidates = append(candidates, e)
	}

	if len(candidates) == 0 {
		return nil
	}

	// Apply filter to candidates
	if c.filter != nil {
		preferred, other := partition(candidates, c.filter, c.info)
		if e := c.pickOne(preferred); e != nil {
			return e
		}
		if c.allowFallback && len(other) > 0 {
			return c.pickOne(other)
		}
		return nil
	}

	return c.pickOne(candidates)
}

// pickOne selects a random endpoint from candidates.
func (c *selectionCtx) pickOne(candidates []endpoint.Endpoint) endpoint.Endpoint {
	switch len(candidates) {
	case 0:
		return nil
	case 1:
		return candidates[0]
	default:
		if c.rnd == nil {
			return candidates[0]
		}
		return candidates[c.rnd.Int(len(candidates))]
	}
}

// replacementEndpoint is a compatibility wrapper for findReplacement.
func replacementEndpoint(
	discovered []endpoint.Endpoint,
	active []conn.Conn,
	excluded endpoint.Key,
	filter balancerConfig.Filter,
	info balancerConfig.Info,
	allowFallback bool,
	rnd xrand.Rand,
) endpoint.Endpoint {
	return newSelectionCtx(active, discovered, 0, filter, info, allowFallback, rnd).findReplacement(excluded)
}

func endpointByNodeID(endpoints []endpoint.Endpoint, nodeID uint32) endpoint.Endpoint {
	for _, e := range endpoints {
		if e.NodeID() == nodeID {
			return e
		}
	}

	return nil
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

func swap(i, j int) {
	i, j = j, i
}
