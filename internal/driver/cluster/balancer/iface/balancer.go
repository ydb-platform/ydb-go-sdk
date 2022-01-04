package iface

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/info"
)

// Element is an empty interface that holds some Balancer specific
// data.
type Element interface{}

// Balancer is an interface that implements particular load-balancing
// algorithm.
//
// Balancer methods called synchronized. That is, implementations must not
// provide additional goroutine safety.
type Balancer interface {
	// Next returns next connection for request.
	// Next MUST not return nil if it has at least one connection.
	Next() conn.Conn

	// Insert inserts new connection.
	Insert(conn.Conn) Element

	// Update updates previously inserted connection.
	Update(Element, info.Info)

	// Remove removes previously inserted connection.
	Remove(Element)

	// Contains returns true if Balancer contains requested element.
	Contains(Element) bool
}
