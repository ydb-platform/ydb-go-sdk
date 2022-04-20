package multi

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

func Balancer(opts ...Option) balancer.Balancer {
	m := new(multi)
	for _, opt := range opts {
		opt(m)
	}
	return m
}

type multi struct {
	balancers []balancer.Balancer
	filters   []func(conn.Conn) bool
}

func (m *multi) Create(conns []conn.Conn) balancer.Balancer {
	newBalancers := make([]balancer.Balancer, len(m.balancers))
	for i, balancer := range m.balancers {
		balancerConns := make([]conn.Conn, 0, len(conns))

		filter := m.filters[i]
		for _, conn := range conns {
			if filter(conn) {
				balancerConns = append(balancerConns, conn)
			}
		}
		newBalancers[i] = balancer.Create(balancerConns)
	}

	return &multi{
		balancers: newBalancers,
		filters:   m.filters,
	}
}

func (m *multi) NeedRefresh(ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}
	ctx, ctxCancel := context.WithCancel(ctx)
	defer ctxCancel()

	// buffered channel need for prevent wait goroutines leak after function exit
	needRefreshChannels := make(chan bool, len(m.balancers))

	waitRefreshSignal := func(b balancer.Balancer) {
		go func() {
			needRefreshChannels <- b.NeedRefresh(ctx)
		}()
	}

	for _, b := range m.balancers {
		waitRefreshSignal(b)
	}

	for range m.balancers {
		select {
		case <-ctx.Done():
			return false
		case needRefresh := <-needRefreshChannels:
			if needRefresh {
				return true
			}
		}
	}

	return false
}

func (m *multi) Next(ctx context.Context, allowBanned bool) conn.Conn {
	for _, b := range m.balancers {
		if c := b.Next(ctx, allowBanned); c != nil {
			return c
		}
	}
	return nil
}

func WithBalancer(b balancer.Balancer, filter func(cc conn.Conn) bool) Option {
	return func(m *multi) {
		m.balancers = append(m.balancers, b)
		m.filters = append(m.filters, filter)
	}
}

type Option func(*multi)
