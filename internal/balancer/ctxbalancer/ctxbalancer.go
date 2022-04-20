package ctxbalancer

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

type CtxBalancer struct {
	connMap map[uint32]conn.Conn
}

func Balancer(conns []conn.Conn) balancer.Balancer {
	connMap := make(map[uint32]conn.Conn, len(conns))
	for _, conn := range conns {
		if nodeId := conn.Endpoint().NodeID(); nodeId > 0 {
			connMap[nodeId] = conn
		}
	}
	return &CtxBalancer{connMap: connMap}
}

func (c *CtxBalancer) Next(ctx context.Context, allowBanned bool) conn.Conn {
	if e, ok := ContextEndpoint(ctx); ok {
		if cc, ok := c.connMap[e.NodeID()]; ok && balancer.IsOkConnection(cc, true) {
			if err := cc.Ping(ctx); err == nil {
				return cc
			}
		}
	}
	return nil
}

func (c *CtxBalancer) Create(conns []conn.Conn) balancer.Balancer {
	return Balancer(conns)
}

func (c *CtxBalancer) NeedRefresh(ctx context.Context) bool {
	<-ctx.Done()
	return false
}
