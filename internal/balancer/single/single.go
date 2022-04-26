package single

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

func Balancer(c conn.Conn) balancer.Balancer {
	return &single{
		conn: c,
	}
}

type single struct {
	conn conn.Conn
}

func (b *single) Create(conns []conn.Conn) balancer.Balancer {
	connCount := len(conns)
	switch {
	case connCount == 0:
		return &single{}
	case connCount == 1:
		return &single{
			conn: conns[0],
		}
	default:
		panic("ydb: single Conn Balancer: must not conains more one value")
	}
}

func (b *single) Next(ctx context.Context, opts ...balancer.NextOption) conn.Conn {
	b.checkIfNeedRefresh(ctx, opts...)

	return b.conn
}

func (b *single) Conn() conn.Conn {
	return b.conn
}

func (b *single) checkIfNeedRefresh(ctx context.Context, opts ...balancer.NextOption) {
	opt := balancer.NewNextOptions(opts...)
	if b.conn != nil && !balancer.IsOkConnection(b.conn, opt.WantPessimized) {
		opt.Discovery(ctx)
	}
}

func IsSingle(i balancer.Balancer) bool {
	_, ok := i.(*single)
	return ok
}
