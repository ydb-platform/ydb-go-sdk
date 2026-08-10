package balancer

import (
	"context"
	"slices"

	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/strategy"
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
	groups   [][]conn.Conn
	all      []conn.Conn

	quarantine []conn.Conn

	allowFallback bool
	balancer      strategy.Balancer
	info          strategy.Info

	rand xrand.Rand
}

func newConnectionsStateWithBalancer(
	conns []conn.Conn,
	balancer strategy.Balancer,
	info strategy.Info,
	quarantine []conn.Conn,
) *connectionsState {
	if balancer == nil {
		balancer = strategy.RandomChoice()
	}
	groups := balancer.Filter(info, xslices.Transform(conns, func(connection conn.Conn) endpoint.Endpoint {
		return connection.Endpoint()
	}))

	return newConnectionsStateWithBalancerGroups(conns, balancer, info, groups, quarantine)
}

func newConnectionsStateWithBalancerGroups(
	conns []conn.Conn,
	balancer strategy.Balancer,
	info strategy.Info,
	endpointGroups [][]endpoint.Endpoint,
	quarantine []conn.Conn,
) *connectionsState {
	if balancer == nil {
		balancer = strategy.RandomChoice()
	}

	res := &connectionsState{
		connByNodeID: connsToNodeIDMap(conns),
		rand:         xrand.New(xrand.WithLock()),
		quarantine:   quarantine,
		balancer:     balancer,
		info:         info,
	}

	res.all = conns
	res.groups = make([][]conn.Conn, 0, len(endpointGroups))
	for _, group := range endpointGroups {
		connections := connectionsForEndpoints(conns, group)
		if len(connections) > 0 {
			res.groups = append(res.groups, connections)
		}
	}
	if len(res.groups) > 0 {
		res.prefer = res.groups[0]
	}
	if len(res.groups) > 1 {
		for _, group := range res.groups[1:] {
			res.fallback = append(res.fallback, group...)
		}
	}
	res.allowFallback = len(res.groups) > 1

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

	nextCtx := strategy.NextContext{Info: s.info, Rand: s.rand}
	for _, group := range s.groups {
		c, failed := s.balancer.Next(ctx, nextCtx, group, false)
		failedCount += failed
		if c != nil {
			return c, failedCount
		}
	}

	candidates := make([]conn.Conn, 0, len(s.prefer)+len(s.fallback))
	for _, group := range s.groups {
		candidates = append(candidates, group...)
	}
	c, _ := s.balancer.Next(ctx, nextCtx, candidates, true)

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

func connectionsForEndpoints(conns []conn.Conn, endpoints []endpoint.Endpoint) []conn.Conn {
	if len(endpoints) == 0 {
		return nil
	}

	byKey := make(map[endpoint.Key]conn.Conn, len(conns))
	for _, connection := range conns {
		byKey[connection.Endpoint().Key()] = connection
	}

	result := make([]conn.Conn, 0, len(endpoints))
	for _, candidate := range endpoints {
		if connection := byKey[candidate.Key()]; connection != nil {
			result = append(result, connection)
		}
	}

	return result
}

func endpointsForConnections(conns []conn.Conn, endpoints []endpoint.Endpoint) []endpoint.Endpoint {
	if len(conns) == 0 || len(endpoints) == 0 {
		return nil
	}

	byKey := make(map[endpoint.Key]endpoint.Endpoint, len(endpoints))
	for _, candidate := range endpoints {
		byKey[candidate.Key()] = candidate
	}

	result := make([]endpoint.Endpoint, 0, len(conns))
	for _, connection := range conns {
		if candidate := byKey[connection.Endpoint().Key()]; candidate != nil {
			result = append(result, candidate)
		}
	}

	return result
}

func sortPreferConnections(
	conns []conn.Conn,
	filter balancerConfig.Filter,
	info balancerConfig.Info,
	allowFallback bool,
) (prefer, fallback []conn.Conn) {
	if filter == nil {
		return conns, nil
	}

	prefer = make([]conn.Conn, 0, len(conns))
	if allowFallback {
		fallback = make([]conn.Conn, 0, len(conns))
	}

	for _, c := range conns {
		if filter.Allow(info, c.Endpoint()) {
			prefer = append(prefer, c)
		} else if allowFallback {
			fallback = append(fallback, c)
		}
	}

	return prefer, fallback
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
