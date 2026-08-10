package balancer

import (
	"context"
	"math"
	"slices"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/strategy"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
)

type connectionsState struct {
	connByNodeID  map[uint32]conn.Conn
	connByKey     map[endpoint.Key]conn.Conn
	endpointByKey map[endpoint.Key]endpoint.Endpoint
	activeKeys    map[endpoint.Key]struct{}

	estimates []strategy.Estimation
	elector   *endpointElector

	preferredCount int
	all            []conn.Conn
	quarantine     []conn.Conn

	rand xrand.Rand
}

func newConnectionsStateWithBalancer(
	conns []conn.Conn,
	estimator strategy.Estimator,
	info strategy.Info,
	quarantine []conn.Conn,
) *connectionsState {
	if estimator == nil {
		estimator = strategy.RandomChoice()
	}
	endpoints := make([]endpoint.Endpoint, 0, len(conns))
	for _, connection := range conns {
		endpoints = append(endpoints, connection.Endpoint())
	}

	return newConnectionsStateWithEstimates(
		conns, endpoints, estimator.Estimate(info, endpoints), endpointKeySet(endpoints), quarantine, info.Rand,
	)
}

func newConnectionsStateWithEstimates(
	conns []conn.Conn,
	endpoints []endpoint.Endpoint,
	estimates []strategy.Estimation,
	activeKeys map[endpoint.Key]struct{},
	quarantine []conn.Conn,
	rand xrand.Rand,
) *connectionsState {
	if rand == nil {
		rand = xrand.New(xrand.WithLock())
	}
	if activeKeys == nil {
		activeKeys = endpointKeySet(endpoints)
	}
	result := &connectionsState{
		connByNodeID:  connsToNodeIDMap(conns),
		connByKey:     make(map[endpoint.Key]conn.Conn, len(conns)),
		endpointByKey: make(map[endpoint.Key]endpoint.Endpoint, len(endpoints)),
		activeKeys:    cloneEndpointKeySet(activeKeys),
		estimates:     append([]strategy.Estimation(nil), estimates...),
		all:           append([]conn.Conn(nil), conns...),
		quarantine:    quarantine,
		rand:          rand,
	}
	for _, connection := range conns {
		result.connByKey[connection.Endpoint().Key()] = connection
	}
	for _, candidate := range endpoints {
		result.endpointByKey[candidate.Key()] = candidate
	}
	result.elector = newEndpointElector(result.estimates, result.connByKey, result.activeKeys, rand)
	result.preferredCount = preferredConnectionCount(result.estimates, result.connByKey)

	return result
}

func endpointKeySet(endpoints []endpoint.Endpoint) map[endpoint.Key]struct{} {
	result := make(map[endpoint.Key]struct{}, len(endpoints))
	for _, candidate := range endpoints {
		result[candidate.Key()] = struct{}{}
	}

	return result
}

func cloneEndpointKeySet(keys map[endpoint.Key]struct{}) map[endpoint.Key]struct{} {
	if keys == nil {
		return nil
	}
	result := make(map[endpoint.Key]struct{}, len(keys))
	for key := range keys {
		result[key] = struct{}{}
	}

	return result
}

func (s *connectionsState) ActiveKeys() map[endpoint.Key]struct{} {
	if s == nil {
		return nil
	}

	return cloneEndpointKeySet(s.activeKeys)
}

func preferredConnectionCount(
	estimates []strategy.Estimation,
	connections map[endpoint.Key]conn.Conn,
) int {
	minimum := uint64(math.MaxUint64)
	found := false
	for _, estimation := range estimates {
		if estimation.Weight > 0 && connections[estimation.Key] != nil {
			minimum = min(minimum, estimation.Penalty)
			found = true
		}
	}
	if !found {
		return 0
	}

	seen := make(map[endpoint.Key]struct{}, len(estimates))
	count := 0
	for _, estimation := range estimates {
		if estimation.Weight == 0 || estimation.Penalty != minimum || connections[estimation.Key] == nil {
			continue
		}
		if _, duplicate := seen[estimation.Key]; duplicate {
			continue
		}
		seen[estimation.Key] = struct{}{}
		count++
	}

	return count
}

func (s *connectionsState) PreferredCount() int {
	return s.preferredCount
}

func (s *connectionsState) All() []conn.Conn {
	if s == nil {
		return nil
	}

	return slices.Clone(s.all)
}

func (s *connectionsState) Endpoints() []endpoint.Endpoint {
	if s == nil {
		return nil
	}
	result := make([]endpoint.Endpoint, 0, len(s.endpointByKey))
	for _, candidate := range s.endpointByKey {
		result = append(result, candidate)
	}

	return result
}

func (s *connectionsState) Estimations() []strategy.Estimation {
	if s == nil {
		return nil
	}

	return append([]strategy.Estimation(nil), s.estimates...)
}

func (s *connectionsState) Endpoint(key endpoint.Key) endpoint.Endpoint {
	if s == nil {
		return nil
	}

	return s.endpointByKey[key]
}

func (s *connectionsState) Connection(key endpoint.Key) conn.Conn {
	if s == nil {
		return nil
	}

	return s.connByKey[key]
}

func (s *connectionsState) NextEndpoint(ctx context.Context) (
	key endpoint.Key,
	connection conn.Conn,
	allowBanned bool,
	ok bool,
) {
	if ctx.Err() != nil {
		return endpoint.Key{}, nil, false, false
	}
	key, allowBanned, ok = s.elector.Next()
	if !ok {
		return endpoint.Key{}, nil, false, false
	}

	return key, s.connByKey[key], allowBanned, true
}

func (s *connectionsState) GetConnection(ctx context.Context) (_ conn.Conn, failedCount int) {
	if ctx.Err() != nil {
		return nil, 0
	}
	if connection := s.preferConnection(ctx); connection != nil {
		return connection, 0
	}
	if _, hasNode := endpoint.ContextNodeID(ctx); hasNode && !endpoint.ContextFallback(ctx) {
		return nil, 0
	}

	for {
		key, connection, allowBanned, ok := s.NextEndpoint(ctx)
		if !ok || connection == nil {
			return nil, failedCount
		}
		if isOkConnection(connection, allowBanned) {
			return connection, failedCount
		}
		failedCount++
		s.Pessimize(key)
	}
}

func (s *connectionsState) preferConnection(ctx context.Context) conn.Conn {
	if nodeID, hasPreferEndpoint := endpoint.ContextNodeID(ctx); hasPreferEndpoint {
		connection := s.connByNodeID[nodeID]
		if connection != nil && isOkConnection(connection, false) {
			return connection
		}
	}

	return nil
}

func (s *connectionsState) Pessimize(key endpoint.Key) {
	if s != nil && s.elector != nil {
		s.elector.Pessimize(key)
	}
}

func (s *connectionsState) Unpessimize(key endpoint.Key) {
	if s != nil && s.elector != nil {
		s.elector.Unpessimize(key)
	}
}

func (s *connectionsState) selectRandomConnection(conns []conn.Conn, allowBanned bool) (c conn.Conn, failedConns int) {
	connCount := len(conns)
	if connCount == 0 {
		return nil, 0
	}
	if c := conns[s.rand.Int(connCount)]; isOkConnection(c, allowBanned) {
		return c, 0
	}

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
			return c, failedConns
		}
		failedConns++
	}

	return nil, failedConns
}

func connsToNodeIDMap(conns []conn.Conn) map[uint32]conn.Conn {
	if len(conns) == 0 {
		return nil
	}
	nodes := make(map[uint32]conn.Conn, len(conns))
	for _, connection := range conns {
		nodes[connection.Endpoint().NodeID()] = connection
	}

	return nodes
}

func previousEndpoints(connections []conn.Conn) []strategy.PreviousEndpoint {
	result := make([]strategy.PreviousEndpoint, 0, len(connections))
	for _, connection := range connections {
		if connection == nil {
			continue
		}
		result = append(result, strategy.PreviousEndpoint{
			Key:    connection.Endpoint().Key(),
			Banned: connection.State() == state.Banned,
		})
	}

	return result
}

func isOkConnection(connection conn.Conn, bannedIsOK bool) bool {
	switch connection.State() {
	case state.Online, state.Created, state.Offline:
		return true
	case state.Banned:
		return bannedIsOK
	default:
		return false
	}
}
