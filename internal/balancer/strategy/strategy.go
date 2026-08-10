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
	// Filter returns endpoint groups in the order in which admission policies
	// should consider them. Groups omitted from the result are not eligible for
	// admission by an outer limiting strategy.
	Filter(info Info, endpoints []endpoint.Endpoint) [][]endpoint.Endpoint

	// Select returns the endpoints which should be held in the active connection
	// state for a discovery generation.
	Select(ctx SelectContext, endpoints []endpoint.Endpoint) []endpoint.Endpoint

	// Next selects a connection from an already reconciled connection state.
	Next(ctx context.Context, nextCtx NextContext, connections []conn.Conn, allowBanned bool) (
		connection conn.Conn,
		failed int,
	)

	Requirements() Requirements
	String() string
}

type Requirements struct {
	DetectNearestDC bool
	Limited         bool
	SingleConn      bool
}

type Info struct {
	SelfLocation string
}

type SelectContext struct {
	Previous []conn.Conn
	Info     Info
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

func Prefer(child Balancer, filter Filter, allowFallback, detectNearestDC bool) Balancer {
	return prefer{
		child:           normalize(child),
		filter:          filter,
		allowFallback:   allowFallback,
		detectNearestDC: detectNearestDC,
	}
}

func WithMaxConnections(child Balancer, limit int) Balancer {
	if limit < 0 {
		limit = 0
	}

	return maxConnections{
		child: normalize(child),
		limit: limit,
	}
}

func normalize(balancer Balancer) Balancer {
	if balancer == nil {
		return RandomChoice()
	}

	return balancer
}

type randomChoice struct{}

func (randomChoice) Filter(_ Info, endpoints []endpoint.Endpoint) [][]endpoint.Endpoint {
	return [][]endpoint.Endpoint{endpoints}
}

func (randomChoice) Select(_ SelectContext, endpoints []endpoint.Endpoint) []endpoint.Endpoint {
	return endpoints
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

func (randomChoice) Requirements() Requirements {
	return Requirements{}
}

func (randomChoice) String() string {
	return "RandomChoice"
}

type singleConn struct{}

func (singleConn) Filter(_ Info, endpoints []endpoint.Endpoint) [][]endpoint.Endpoint {
	return [][]endpoint.Endpoint{endpoints}
}

func (singleConn) Select(_ SelectContext, endpoints []endpoint.Endpoint) []endpoint.Endpoint {
	return endpoints
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

func (singleConn) Requirements() Requirements {
	return Requirements{SingleConn: true}
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
