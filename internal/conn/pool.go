package conn

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
)

type Pool interface {
	closer.Closer
	Pessimizer
	PoolGetter
}

type PoolGetter interface {
	GetConn(endpoint endpoint.Endpoint) Conn
}

type Pessimizer interface {
	Pessimize(ctx context.Context, e endpoint.Endpoint) error
}

type PoolGetterCloser interface {
	PoolGetter
	closer.Closer
}

type PoolConfig interface {
	ConnectionTTL() time.Duration
	GrpcDialOptions() []grpc.DialOption
}

type pool struct {
	config Config
	mtx    sync.RWMutex
	opts   []grpc.DialOption
	conns  map[string]Conn
	done   chan struct{}
}

func (p *pool) Pessimize(ctx context.Context, e endpoint.Endpoint) (err error) {
	p.mtx.RLock()
	defer p.mtx.RUnlock()
	cc, ok := p.conns[e.Address()]
	if !ok {
		cc.SetState(ctx, Banned)
		return nil
	}
	return errors.Errorf(0, "pessimize failed: unknown endpoint %v", e)
}

func (p *pool) GetConn(e endpoint.Endpoint) Conn {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	if cc, ok := p.conns[e.Address()]; ok {
		return cc
	}
	cc := New(e, p.config)
	p.conns[e.Address()] = cc
	return cc
}

func (p *pool) Close(ctx context.Context) error {
	close(p.done)

	p.mtx.Lock()
	defer p.mtx.Unlock()

	var issues []error
	for a, c := range p.conns {
		if err := c.Close(ctx); err != nil {
			issues = append(issues, err)
		}
		delete(p.conns, a)
	}

	if len(issues) > 0 {
		return errors.NewWithIssues("connection pool close failed", issues...)
	}

	return nil
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
		conns:  make(map[string]Conn),
		done:   make(chan struct{}),
	}
	if ttl := config.ConnectionTTL(); ttl > 0 {
		go p.connCloser(ctx, ttl/10)
	}
	return p
}
