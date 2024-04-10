package mock

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
)

var _ endpoint.Info = (*Conn)(nil)

type Conn struct {
	EndpointField endpoint.Info
	StateField    conn.State
}

func (c *Conn) String() string {
	return c.EndpointField.String()
}

func (c *Conn) NodeID() uint32 {
	return c.EndpointField.NodeID()
}

func (c *Conn) Address() string {
	return c.EndpointField.Address()
}

func (c *Conn) Location() string {
	return c.EndpointField.Location()
}

func (c *Conn) LastUpdated() time.Time {
	return c.EndpointField.LastUpdated()
}

func (c *Conn) LoadFactor() float32 {
	return c.EndpointField.LoadFactor()
}

func (c *Conn) Ready() bool {
	return conn.Ready(c.StateField)
}

func (c *Conn) State() conn.State {
	return c.StateField
}
