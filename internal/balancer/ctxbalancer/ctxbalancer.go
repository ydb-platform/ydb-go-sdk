package ctxbalancer

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

type ctxBalancer struct {
	connMap map[uint32]conn.Conn
}

func Balancer(conns []conn.Conn) balancer.Balancer {
	connMap := make(map[uint32]conn.Conn, len(conns))
	for _, conn := range conns {
		if nodeID := conn.Endpoint().NodeID(); nodeID > 0 {
			connMap[nodeID] = conn
		}
	}
	return &ctxBalancer{connMap: connMap}
}

// Next of ctxBalancer return the connection
func (c *ctxBalancer) Next(ctx context.Context, _allowBanned bool) conn.Conn {
	if e, ok := ContextEndpoint(ctx); ok {
		if cc, ok := c.connMap[e.NodeID()]; ok && balancer.IsOkConnection(cc, true) {
			if err := cc.Ping(ctx); err == nil {
				return cc
			}
		}
	}
	return nil
}

func (c *ctxBalancer) Create(conns []conn.Conn) balancer.Balancer {
	return Balancer(conns)
}

func (c *ctxBalancer) NeedRefresh(ctx context.Context) bool {
	return false
}
