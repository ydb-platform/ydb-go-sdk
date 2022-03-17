package conn

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Pool interface {
	Creator
	Getter
	Taker
	Releaser
	Pessimizer
}

type Getter interface {
	Get(ctx context.Context, endpoint endpoint.Endpoint) Conn
}

type Creator interface {
	// Create Conn but don't put it into pool
	Create(ctx context.Context, endpoint endpoint.Endpoint) Conn
}

type Taker interface {
	Take(ctx context.Context) error
}

type Releaser interface {
	Release(ctx context.Context) error
}

type Pessimizer interface {
	Pessimize(ctx context.Context, cc Conn, cause error)
}

type PoolConfig interface {
	ConnectionTTL() time.Duration
	GrpcDialOptions() []grpc.DialOption
}

type pool struct {
	sync.RWMutex
	usages int64
	config Config
	opts   []grpc.DialOption
	conns  map[string]*conn
	done   chan struct{}
}

func (p *pool) Create(_ context.Context, endpoint endpoint.Endpoint) Conn {
	cc := newConn(endpoint, p.config)
	return cc
}

func (p *pool) Get(_ context.Context, endpoint endpoint.Endpoint) Conn {
	p.Lock()
	defer p.Unlock()

	var (
		address = endpoint.Address()
		cc      *conn
		has     bool
	)

	if cc, has = p.conns[address]; has {
		cc.incUsages()
		return cc
	}

	cc = newConn(endpoint, p.config, withOnClose(p.remove))

	p.conns[address] = cc

	return cc
}

func (p *pool) remove(c *conn) {
	p.Lock()
	defer p.Unlock()
	delete(p.conns, c.Endpoint().Address())
}

func (p *pool) Pessimize(ctx context.Context, cc Conn, cause error) {
	e := cc.Endpoint().Copy()

	p.RLock()
	defer p.RUnlock()

	cc, ok := p.conns[e.Address()]
	if !ok {
		return
	}

	trace.DriverOnPessimizeNode(
		trace.ContextDriver(ctx).Compose(p.config.Trace()),
		&ctx,
		e,
		cc.GetState(),
		cause,
	)(cc.SetState(Banned))
}

func (p *pool) Take(context.Context) error {
	atomic.AddInt64(&p.usages, 1)
	return nil
}

func (p *pool) Release(ctx context.Context) error {
	if atomic.AddInt64(&p.usages, -1) > 0 {
		return nil
	}

	close(p.done)

	var issues []error
	conns := p.collectConns()
	for _, c := range conns {
		if err := c.Close(ctx); err != nil {
			issues = append(issues, err)
		}
	}

	if len(issues) > 0 {
		return errors.NewWithIssues("connection pool close failed", issues...)
	}

	return nil
}

func (p *pool) connParker(ctx context.Context, ttl, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-p.done:
			return
		case <-ticker.C:
			conns := p.collectConns()
			for _, c := range conns {
				if time.Since(c.LastUsage()) > ttl {
					_ = c.park(ctx)
				}
			}
		}
	}
}

func (p *pool) collectConns() []*conn {
	p.RLock()
	defer p.RUnlock()
	conns := make([]*conn, 0, len(p.conns))
	for _, c := range p.conns {
		conns = append(conns, c)
	}
	return conns
}

func NewPool(
	ctx context.Context,
	config Config,
) Pool {
	p := &pool{
		usages: 1,
		config: config,
		opts:   config.GrpcDialOptions(),
		conns:  make(map[string]*conn),
		done:   make(chan struct{}),
	}
	if ttl := config.ConnectionTTL(); ttl > 0 {
		go p.connParker(ctx, ttl, ttl/2)
	}
	return p
}
