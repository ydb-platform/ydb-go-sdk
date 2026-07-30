package balancer

import (
	"context"
	"slices"

	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xslices"
)

type connectionsState struct {
	connByNodeID map[uint32]conn.Conn

	prefer   []conn.Conn
	fallback []conn.Conn
	all      []conn.Conn

	quarantine []conn.Conn

	allowFallback bool

	rand xrand.Rand
}

func newConnectionsState(
	conns []conn.Conn,
	filter balancerConfig.Filter,
	info balancerConfig.Info,
	allowFallback bool,
	quarantine []conn.Conn,
) *connectionsState {
	res := &connectionsState{
		connByNodeID:  connsToNodeIDMap(conns),
		rand:          xrand.New(xrand.WithLock()),
		quarantine:    quarantine,
		allowFallback: allowFallback,
	}

	res.prefer, res.fallback = sortPreferConnections(conns, filter, info, allowFallback)
	res.all = conns

	return res
}

func (s *connectionsState) PreferredCount() int {
	return len(s.prefer)
}

func (s *connectionsState) All() []conn.Conn {
	if s == nil {
		return nil
	}

	return slices.Clone(s.all)
}

func (s *connectionsState) GetConnection(ctx context.Context) (_ conn.Conn, failedCount int) {
	if err := ctx.Err(); err != nil {
		return nil, 0
	}

	if c := s.preferConnection(ctx); c != nil {
		return c, 0
	}
	if _, hasNode := endpoint.ContextNodeID(ctx); hasNode && !endpoint.ContextFallback(ctx) {
		return nil, 0
	}

	return s.GetConnectionUnpinned(ctx)
}

// GetConnectionUnpinned selects a connection without checking context node pin.
// Use when the caller already handled (or skipped) pin affinity.
func (s *connectionsState) GetConnectionUnpinned(ctx context.Context) (_ conn.Conn, failedCount int) {
	if err := ctx.Err(); err != nil {
		return nil, 0
	}

	try := func(conns []conn.Conn) conn.Conn {
		c, tryFailed := s.selectRandomConnection(conns, false)
		failedCount += tryFailed

		return c
	}

	if c := try(s.prefer); c != nil {
		return c, failedCount
	}

	if c := try(s.fallback); c != nil {
		return c, failedCount
	}

	lastResort := s.all
	if !s.allowFallback && len(s.prefer) != len(s.all) {
		lastResort = s.prefer
	}

	c, _ := s.selectRandomConnection(lastResort, true)

	return c, failedCount
}

func (s *connectionsState) preferConnection(ctx context.Context) conn.Conn {
	if nodeID, hasPreferEndpoint := endpoint.ContextNodeID(ctx); hasPreferEndpoint {
		c := s.connByNodeID[nodeID]
		if c != nil && isOkConnection(c, false) {
			return c
		}
	}

	return nil
}

func (s *connectionsState) selectRandomConnection(conns []conn.Conn, allowBanned bool) (c conn.Conn, failedConns int) {
	connCount := len(conns)
	if connCount == 0 {
		// return for empty list need for prevent panic in fast path
		return nil, 0
	}

	// fast path
	if c := conns[s.rand.Int(connCount)]; isOkConnection(c, allowBanned) {
		return c, 0
	}

	// shuffled indexes slices need for guarantee about every connection will check
	indexes := make([]int, connCount)
	for index := range indexes {
		indexes[index] = index
	}
	s.rand.Shuffle(connCount, func(i, j int) {
		indexes[i], indexes[j] = indexes[j], indexes[i]
	})

	for _, index := range indexes {
		c := conns[index]
		if isOkConnection(c, allowBanned) {
			return c, 0
		}
		failedConns++
	}

	return nil, failedConns
}

// ApplyDiscovery updates the connection state after a discovery round.
// It moves old active connections that are no longer in selected to quarantine,
// puts old quarantine back to pool (after 2-round cycle), and unbans new active.
func (s *connectionsState) ApplyDiscovery(
	ctx context.Context,
	pool poolInterface,
	selected []endpoint.Endpoint,
	active []conn.Conn,
	quarantine []conn.Conn,
) (newQuarantine []conn.Conn, newActive []conn.Conn) {
	// Build set of selected endpoint keys
	selectedKeys := make(map[endpoint.Key]struct{}, len(selected))
	for _, e := range selected {
		selectedKeys[e.Key()] = struct{}{}
	}

	// Move old active connections that are no longer selected to quarantine
	newQuarantine = quarantine
	for _, cc := range active {
		if _, ok := selectedKeys[cc.Endpoint().Key()]; !ok {
			newQuarantine = append(newQuarantine, cc)
		}
	}

	// Get connections for selected endpoints
	newActive = xslices.Filter(
		xslices.Transform(selected, func(e endpoint.Endpoint) conn.Conn {
			return pool.Get(e)
		}),
		func(cc conn.Conn) bool { return cc != nil },
	)

	// Put old quarantine back to pool (they completed the 2-round cycle)
	for _, cc := range quarantine {
		pool.Put(ctx, cc)
	}

	// Unban new active connections
	for _, cc := range newActive {
		cc.Unban(ctx)
	}

	return newQuarantine, newActive
}

// poolInterface abstracts conn.Pool for testing and discovery state updates.
type poolInterface interface {
	Get(e endpoint.Endpoint) conn.Conn
	Put(ctx context.Context, cc conn.Conn)
}

func connsToNodeIDMap(conns []conn.Conn) (nodes map[uint32]conn.Conn) {
	if len(conns) == 0 {
		return nil
	}
	nodes = make(map[uint32]conn.Conn, len(conns))
	for _, c := range conns {
		nodes[c.Endpoint().NodeID()] = c
	}

	return nodes
}

// sortPreferConnections sorts connections into preferred (matching filter) and fallback.
// Uses partition() from select_endpoints.go to avoid duplication.
func sortPreferConnections(
	conns []conn.Conn,
	filter balancerConfig.Filter,
	info balancerConfig.Info,
	allowFallback bool,
) (prefer, fallback []conn.Conn) {
	if filter == nil {
		return conns, nil
	}

	// Convert connections to endpoints for partition
	endpoints := make([]endpoint.Endpoint, len(conns))
	for i, c := range conns {
		endpoints[i] = c.Endpoint()
	}

	preferred, other := partition(endpoints, filter, info)

	// Convert back to connections
	prefer = make([]conn.Conn, 0, len(preferred))
	for _, e := range preferred {
		prefer = append(prefer, findConnByEndpoint(conns, e))
	}

	if allowFallback {
		fallback = make([]conn.Conn, 0, len(other))
		for _, e := range other {
			fallback = append(fallback, findConnByEndpoint(conns, e))
		}
	}

	return prefer, fallback
}

func findConnByEndpoint(conns []conn.Conn, e endpoint.Endpoint) conn.Conn {
	for _, c := range conns {
		if c != nil && c.Endpoint().Key() == e.Key() {
			return c
		}
	}

	return nil
}

func isOkConnection(c conn.Conn, bannedIsOk bool) bool {
	switch c.State() {
	case state.Online, state.Created, state.Offline:
		return true
	case state.Banned:
		return bannedIsOk
	default:
		return false
	}
}
