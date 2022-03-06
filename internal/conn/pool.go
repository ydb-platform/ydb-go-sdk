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
	PoolGetter
}

type PoolGetter interface {
	GetConn(endpoint endpoint.Endpoint) Conn
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
	config      Config
	mtx         sync.RWMutex
	opts        []grpc.DialOption
	conns       map[string]Conn
	done        chan struct{}
	onPessimize func(e endpoint.Endpoint)
}

func (p *pool) GetConn(e endpoint.Endpoint) Conn {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	if cc, ok := p.conns[e.Address()]; ok {
		return cc
	}
	cc := New(
		e,
		p.config,
		withOnPessimize(
			p.onPessimize,
		),
		withOnClose(func(c Conn) {
			// conn.Conn.Close() must called on under locked p.mtx
			delete(p.conns, c.Endpoint().Address())
		}),
	)
	p.conns[e.Address()] = cc
	return cc
}

func (p *pool) Close(ctx context.Context) error {
	close(p.done)

	p.mtx.Lock()
	defer p.mtx.Unlock()

	var issues []error
	for _, c := range p.conns {
		if err := c.Close(ctx); err != nil {
			issues = append(issues, err)
		}
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

func NewPool(
	ctx context.Context,
	config Config,
	onPessimize func(e endpoint.Endpoint),
) Pool {
	p := &pool{
		config:      config,
		opts:        config.GrpcDialOptions(),
		conns:       make(map[string]Conn),
		done:        make(chan struct{}),
		onPessimize: onPessimize,
	}
	if ttl := config.ConnectionTTL(); ttl > 0 {
		go p.connCloser(ctx, ttl/10)
	}
	return p
}
