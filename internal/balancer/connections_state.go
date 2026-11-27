package balancer

import (
	"slices"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/policy"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
)

type connectionsState struct {
	connByNodeID map[uint32]conn.Conn
	elector      *endpointElector

	all []conn.Conn
}

// newConnectionsStateWithPriorities takes ownership of the connection and priority slices.
func newConnectionsStateWithPriorities(
	connections []conn.Conn,
	priorities []policy.EndpointPriority,
	rand xrand.Rand,
) *connectionsState {
	connectionsByKey := make(map[endpoint.Key]conn.Conn, len(connections))
	var connectionsByNodeID map[uint32]conn.Conn
	if len(connections) > 0 {
		connectionsByNodeID = make(map[uint32]conn.Conn, len(connections))
	}
	for _, connection := range connections {
		e := connection.Endpoint()
		connectionsByKey[e.Key()] = connection
		connectionsByNodeID[e.NodeID()] = connection
	}

	return &connectionsState{
		connByNodeID: connectionsByNodeID,
		elector:      newEndpointElector(priorities, connectionsByKey, rand),
		all:          connections,
	}
}

func (s *connectionsState) All() []conn.Conn {
	if s == nil {
		return nil
	}

	return slices.Clone(s.all)
}

func (s *connectionsState) preferConnection(nodeID uint32) conn.Conn {
	connection := s.connByNodeID[nodeID]
	if connection != nil && isConnectionStateUsable(connection.State(), false) {
		return connection
	}

	return nil
}
