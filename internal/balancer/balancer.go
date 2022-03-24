package balancer

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

// Element is an empty interface that holds some Balancer specific data.
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

	// Remove removes previously inserted connection.
	Remove(Element) bool

	// Contains returns true if Balancer contains requested element.
	Contains(Element) bool

	// Create makes empty balancer with same implementation
	Create() Balancer
}
