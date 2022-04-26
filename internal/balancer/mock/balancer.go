package mock

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

type BalancerMock struct {
	OnNext   func(ctx context.Context, opts ...balancer.NextOption) conn.Conn
	OnCreate func(conns []conn.Conn) balancer.Balancer
}

func Balancer() *BalancerMock {
	return &BalancerMock{}
}

func (s *BalancerMock) Create(conns []conn.Conn) balancer.Balancer {
	return s.OnCreate(conns)
}

func (s *BalancerMock) Next(ctx context.Context, opts ...balancer.NextOption) conn.Conn {
	return s.OnNext(ctx, opts...)
}
