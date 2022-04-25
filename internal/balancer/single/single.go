package single

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

func Balancer(c conn.Conn) balancer.Balancer {
	return &single{
		conn:        c,
		needRefresh: make(chan struct{}),
	}
}

type single struct {
	conn conn.Conn

	needRefresh      chan struct{}
	needRefreshClose sync.Once
}

func (b *single) Create(conns []conn.Conn) balancer.Balancer {
	connCount := len(conns)
	switch {
	case connCount == 0:
		return &single{
			needRefresh: make(chan struct{}),
		}
	case connCount == 1:
		return &single{
			conn:        conns[0],
			needRefresh: make(chan struct{}),
		}
	default:
		panic("ydb: single Conn Balancer: must not conains more one value")
	}
}

func (b *single) Next(_ context.Context, allowBanned bool) conn.Conn {
	b.checkIfNeedRefresh()

	return b.conn
}

func (b *single) Conn() conn.Conn {
	return b.conn
}

func (b *single) NeedRefresh(ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}

	select {
	case <-ctx.Done():
		return false
	case <-b.needRefresh:
		return true
	}
}

func (b *single) checkIfNeedRefresh() {
	if b.conn != nil && balancer.IsOkConnection(b.conn, false) {
		return
	}
	b.needRefreshClose.Do(func() {
		close(b.needRefresh)
	})
}

func IsSingle(i interface{}) bool {
	_, ok := i.(*single)
	return ok
}
