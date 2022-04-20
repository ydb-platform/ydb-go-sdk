package mock

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

type BalancerMock struct {
	OnNext        func(ctx context.Context, allowBanned bool) conn.Conn
	OnCreate      func(conns []conn.Conn) balancer.Balancer
	OnNeedRefresh func(ctx context.Context) bool
}

func Balancer() *BalancerMock {
	return &BalancerMock{}
}

func (s *BalancerMock) Create(conns []conn.Conn) balancer.Balancer {
	return s.OnCreate(conns)
}

func (s *BalancerMock) Next(ctx context.Context, allowBanned bool) conn.Conn {
	return s.OnNext(ctx, allowBanned)
}

func (s *BalancerMock) NeedRefresh(ctx context.Context) bool {
	return s.OnNeedRefresh(ctx)
}
