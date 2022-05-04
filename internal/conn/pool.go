package conn

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Pool interface {
	Getter
	Taker
	Releaser
	Pessimizer
}

type Getter interface {
	Get(endpoint endpoint.Endpoint) Conn
}

type Taker interface {
	Take(ctx context.Context) error
}

type Releaser interface {
	Release(ctx context.Context) error
}

type Pessimizer interface {
	Pessimize(ctx context.Context, cc Conn, cause error)
	Unpessimize(ctx context.Context, cc Conn)
}

type PoolConfig interface {
	ConnectionTTL() time.Duration
	GrpcDialOptions() []grpc.DialOption
}

type pool struct {
	usages int64
	config Config
	mtx    sync.RWMutex
	opts   []grpc.DialOption
	conns  map[string]*conn
	done   chan struct{}
}

func (p *pool) Get(endpoint endpoint.Endpoint) Conn {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	var (
		address = endpoint.Address()
		cc      *conn
		has     bool
	)

	if cc, has = p.conns[address]; has {
		return cc
	}

	cc = newConn(endpoint, p.config, withOnClose(p.remove))

	p.conns[address] = cc

	return cc
}

func (p *pool) remove(c *conn) {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	delete(p.conns, c.Endpoint().Address())
}

func (p *pool) Pessimize(ctx context.Context, cc Conn, cause error) {
	e := cc.Endpoint().Copy()

	p.mtx.RLock()
	defer p.mtx.RUnlock()

	cc, ok := p.conns[e.Address()]
	if !ok {
		return
	}

	trace.DriverOnPessimizeNode(
		p.config.Trace(),
		&ctx,
		e,
		cc.GetState(),
		cause,
	)(cc.SetState(Banned))
}

func (p *pool) Unpessimize(ctx context.Context, cc Conn) {
	e := cc.Endpoint().Copy()

	p.mtx.RLock()
	defer p.mtx.RUnlock()

	cc, ok := p.conns[e.Address()]
	if !ok {
		return
	}

	trace.DriverOnUnpessimizeNode(p.config.Trace(),
		&ctx,
		e,
		cc.GetState(),
	)(cc.SetState(Online))
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

	p.mtx.RLock()
	conns := make([]closer.Closer, 0, len(p.conns))
	for _, c := range p.conns {
		conns = append(conns, c)
	}
	p.mtx.RUnlock()

	var issues []error
	for _, c := range conns {
		if err := c.Close(ctx); err != nil {
			issues = append(issues, err)
		}
	}

	if len(issues) > 0 {
		return xerrors.WithStackTrace(xerrors.NewWithIssues("connection pool close failed", issues...))
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
			for _, c := range p.collectConns() {
				if time.Since(c.LastUsage()) > ttl {
					switch c.GetState() {
					case Online, Banned:
						_ = c.park(ctx)
					default:
						// nop
					}
				}
			}
		}
	}
}

func (p *pool) collectConns() []*conn {
	p.mtx.RLock()
	defer p.mtx.RUnlock()
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
