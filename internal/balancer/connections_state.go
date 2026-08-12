package balancer

import (
	"context"
	"slices"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/strategy"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
)

type connectionsState struct {
	connByNodeID map[uint32]conn.Conn
	elector      *endpointElector

	all        []conn.Conn
	quarantine []conn.Conn
}

func newConnectionsStateWithPriorities(
	connections []conn.Conn,
	priorities []strategy.EndpointPriority,
	quarantine []conn.Conn,
	rand xrand.Rand,
) *connectionsState {
	connectionsByKey := make(map[endpoint.Key]conn.Conn, len(connections))
	for _, connection := range connections {
		connectionsByKey[connection.Endpoint().Key()] = connection
	}
	result := &connectionsState{
		connByNodeID: connsToNodeIDMap(connections),
		all:          append([]conn.Conn(nil), connections...),
		quarantine:   quarantine,
	}
	result.elector = newEndpointElector(priorities, connectionsByKey, rand)

	return result
}

func (s *connectionsState) All() []conn.Conn {
	if s == nil {
		return nil
	}

	return slices.Clone(s.all)
}

func (s *connectionsState) preferConnection(ctx context.Context) conn.Conn {
	if nodeID, hasPreferredEndpoint := endpoint.ContextNodeID(ctx); hasPreferredEndpoint {
		connection := s.connByNodeID[nodeID]
		if connection != nil && isConnectionStateUsable(connection.State(), false) {
			return connection
		}
	}

	return nil
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
