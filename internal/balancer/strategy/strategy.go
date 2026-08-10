package strategy

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
)

// Balancer is an immutable, composable endpoint-selection strategy.
// Connection ownership and discovery lifecycle remain outside the strategy.
type Balancer interface {
	// Select returns endpoints that should have active connection wrappers.
	// It runs after discovery and before the connection pool is accessed.
	Select(ctx SelectContext, endpoints []endpoint.Endpoint) []endpoint.Endpoint

	// Filter returns endpoint groups in selection order.
	Filter(info Info, endpoints []endpoint.Endpoint) [][]endpoint.Endpoint

	// Next selects a connection from the current connection state.
	Next(ctx context.Context, nextCtx NextContext, connections []conn.Conn, allowBanned bool) (
		connection conn.Conn,
		failed int,
	)

	String() string
}

type Info struct {
	SelfLocation string
}

type SelectContext struct {
	Info     Info
	Previous []conn.Conn
	Rand     xrand.Rand
}

type NextContext struct {
	Info Info
	Rand xrand.Rand
}

type Filter interface {
	Allow(info Info, endpoint endpoint.Info) bool
	String() string
}

func RandomChoice() Balancer {
	return randomChoice{}
}

func SingleConn() Balancer {
	return singleConn{}
}

func Prefer(child Balancer, filter Filter, allowFallback bool) Balancer {
	return prefer{
		child:         normalize(child),
		filter:        filter,
		allowFallback: allowFallback,
	}
}

func PreferNearestDC(child Balancer, filter Filter, allowFallback bool) Balancer {
	return nearestDC{Balancer: Prefer(child, filter, allowFallback)}
}

func normalize(balancer Balancer) Balancer {
	if balancer == nil {
		return RandomChoice()
	}

	return balancer
}

type randomChoice struct{}

func (randomChoice) Select(_ SelectContext, endpoints []endpoint.Endpoint) []endpoint.Endpoint {
	return endpoints
}

func (randomChoice) Filter(_ Info, endpoints []endpoint.Endpoint) [][]endpoint.Endpoint {
	return [][]endpoint.Endpoint{endpoints}
}

func (randomChoice) Next(
	ctx context.Context,
	nextCtx NextContext,
	connections []conn.Conn,
	allowBanned bool,
) (conn.Conn, int) {
	if ctx.Err() != nil || len(connections) == 0 {
		return nil, 0
	}

	if connection := connections[nextCtx.Rand.Int(len(connections))]; isUsable(connection, allowBanned) {
		return connection, 0
	}

	indexes := make([]int, len(connections))
	for index := range indexes {
		indexes[index] = index
	}
	nextCtx.Rand.Shuffle(len(indexes), func(i, j int) {
		indexes[i], indexes[j] = indexes[j], indexes[i]
	})

	failed := 0
	for _, index := range indexes {
		connection := connections[index]
		if isUsable(connection, allowBanned) {
			return connection, 0
		}
		failed++
	}

	return nil, failed
}

func (randomChoice) String() string {
	return "RandomChoice"
}

type singleConn struct{}

func (singleConn) Select(_ SelectContext, endpoints []endpoint.Endpoint) []endpoint.Endpoint {
	return endpoints
}

func (singleConn) Filter(_ Info, endpoints []endpoint.Endpoint) [][]endpoint.Endpoint {
	return [][]endpoint.Endpoint{endpoints}
}

func (singleConn) Next(
	ctx context.Context,
	_ NextContext,
	connections []conn.Conn,
	allowBanned bool,
) (conn.Conn, int) {
	if ctx.Err() != nil || len(connections) == 0 {
		return nil, 0
	}
	if isUsable(connections[0], allowBanned) {
		return connections[0], 0
	}

	return nil, 1
}

func (singleConn) String() string {
	return "SingleConn"
}

func isUsable(connection conn.Conn, bannedIsUsable bool) bool {
	if connection == nil {
		return false
	}

	switch connection.State() {
	case state.Online, state.Created, state.Offline:
		return true
	case state.Banned:
		return bannedIsUsable
	default:
		return false
	}
}
