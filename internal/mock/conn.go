package mock

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
)

type Conn struct {
	EndpointField endpoint.Info
	StateField    conn.State
}

func (c *Conn) Ready() bool {
	return conn.Ready(c.StateField)
}

func (c *Conn) State() conn.State {
	return c.StateField
}

func (c *Conn) Endpoint() endpoint.Info {
	return c.EndpointField
}
