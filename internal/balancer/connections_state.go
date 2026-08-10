package balancer

import (
	"context"
	"slices"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/strategy"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
)

type connectionsState struct {
	connByNodeID map[uint32]conn.Conn
	connByKey    map[endpoint.Key]conn.Conn

	estimates []strategy.Estimation
	elector   *endpointElector

	all        []conn.Conn
	quarantine []conn.Conn
}

func newConnectionsStateWithEstimates(
	connections []conn.Conn,
	estimates []strategy.Estimation,
	quarantine []conn.Conn,
	rand xrand.Rand,
) *connectionsState {
	result := &connectionsState{
		connByNodeID: connsToNodeIDMap(connections),
		connByKey:    make(map[endpoint.Key]conn.Conn, len(connections)),
		estimates:    append([]strategy.Estimation(nil), estimates...),
		all:          append([]conn.Conn(nil), connections...),
		quarantine:   quarantine,
	}
	for _, connection := range connections {
		result.connByKey[connection.Endpoint().Key()] = connection
	}
	result.elector = newEndpointElector(result.estimates, result.connByKey, rand)

	return result
}

func (s *connectionsState) PreferredCount() int {
	if s == nil || s.elector == nil {
		return 0
	}
	preferred, _ := s.elector.PreferenceHealth()

	return preferred
}

func (s *connectionsState) UnavailablePreferredCount() int {
	if s == nil || s.elector == nil {
		return 0
	}
	_, unavailable := s.elector.PreferenceHealth()

	return unavailable
}

func (s *connectionsState) All() []conn.Conn {
	if s == nil {
		return nil
	}

	return slices.Clone(s.all)
}

func (s *connectionsState) NextEndpoint(ctx context.Context) (
	key endpoint.Key,
	connection conn.Conn,
	allowBanned bool,
	ok bool,
) {
	if ctx.Err() != nil || s == nil || s.elector == nil {
		return endpoint.Key{}, nil, false, false
	}
	key, allowBanned, ok = s.elector.Next()
	if !ok {
		return endpoint.Key{}, nil, false, false
	}

	return key, s.connByKey[key], allowBanned, true
}

func (s *connectionsState) preferConnection(ctx context.Context) conn.Conn {
	if nodeID, hasPreferredEndpoint := endpoint.ContextNodeID(ctx); hasPreferredEndpoint {
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

func connsToNodeIDMap(connections []conn.Conn) map[uint32]conn.Conn {
	if len(connections) == 0 {
		return nil
	}
	nodes := make(map[uint32]conn.Conn, len(connections))
	for _, connection := range connections {
		nodes[connection.Endpoint().NodeID()] = connection
	}

	return nodes
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
