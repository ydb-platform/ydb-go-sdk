package conn

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
)

type Pool interface {
	closer.Closer

	Get(endpoint endpoint.Endpoint) Conn
	Pessimize(ctx context.Context, e endpoint.Endpoint) error
}

type PoolConfig interface {
	ConnectionTTL() time.Duration
	GrpcDialOptions() []grpc.DialOption
}

type pool struct {
	config Config
	mtx    sync.RWMutex
	opts   []grpc.DialOption
	conns  map[endpoint.Endpoint]Conn
	done   chan struct{}
}

func (p *pool) Pessimize(ctx context.Context, e endpoint.Endpoint) error {
	p.mtx.RLock()
	defer p.mtx.RUnlock()
	if cc, ok := p.conns[e]; ok {
		cc.SetState(ctx, Banned)
		return nil
	}
	panic(fmt.Sprintf("unknown endpoint %v", e))
}

func (p *pool) Get(endpoint endpoint.Endpoint) Conn {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	if cc, ok := p.conns[endpoint]; ok {
		return cc
	}
	cc := New(endpoint, p.config)
	p.conns[endpoint] = cc
	return cc
}

func (p *pool) Close(ctx context.Context) error {
	close(p.done)
	var issues []error
	p.mtx.Lock()
	defer p.mtx.Unlock()
	for a, c := range p.conns {
		if err := c.Close(ctx); err != nil {
			issues = append(issues, err)
		}
		delete(p.conns, a)
	}
	if len(issues) == 0 {
		return nil
	}
	return errors.NewWithIssues("connection pool close failed", issues...)
}

func (p *pool) connCloser(ctx context.Context, interval time.Duration) {
	for {
		select {
		case <-p.done:
			return
		case <-time.After(interval):
			p.mtx.RLock()
			for _, c := range p.conns {
				select {
				case <-c.TTL():
					_ = c.Park(ctx)
				default:
					// pass
				}
			}
			p.mtx.RUnlock()
		}
	}
}

func NewPool(ctx context.Context, config Config) Pool {
	p := &pool{
		config: config,
		opts:   config.GrpcDialOptions(),
		conns:  make(map[endpoint.Endpoint]Conn),
		done:   make(chan struct{}),
	}
	if ttl := config.ConnectionTTL(); ttl > 0 {
		go p.connCloser(ctx, ttl/10)
	}
	return p
}
