package conn

import (
	"context"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Pool struct {
	usages int64
	config Config
	mtx    xsync.RWMutex
	opts   []grpc.DialOption
	conns  map[endpoint.Key]*lazyConn
	done   chan struct{}
}

func (p *Pool) Get(endpoint endpoint.Info) Conn {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	var (
		address = endpoint.Address()
		cc      *lazyConn
		has     bool
	)

	key := endpoint.Key()

	if cc, has = p.conns[key]; has {
		return cc
	}

	cc = newConn(endpoint, p.config, withOnClose(p.remove))

	p.conns[key] = cc

	return cc
}

func (p *Pool) remove(c *lazyConn) {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	delete(p.conns, c.Endpoint().Key())
}

func (p *Pool) Attach(ctx context.Context) (finalErr error) {
	onDone := trace.DriverOnPoolAttach(p.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/conn.(*Pool).Attach"),
	)
	defer func() {
		onDone(finalErr)
	}()

	atomic.AddInt64(&p.usages, 1)

	return nil
}

func (p *Pool) Detach(ctx context.Context) (finalErr error) {
	onDone := trace.DriverOnPoolRelease(p.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/conn.(*Pool).Detach"),
	)
	defer func() {
		onDone(finalErr)
	}()

	if atomic.AddInt64(&p.usages, -1) > 0 {
		return nil
	}

	close(p.done)

	var g errgroup.Group
	p.mtx.WithRLock(func() {
		for key := range p.conns {
			conn := p.conns[key]
			g.Go(func() error {
				return conn.Close(ctx)
			})
		}
	})

	if err := g.Wait(); err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (p *Pool) connParker(ctx context.Context, ttl, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-p.done:
			return
		case <-ticker.C:
			for _, c := range p.collectConns() {
				if time.Since(c.lastUsage.Get()) > ttl {
					switch c.State() {
					case connectivity.TransientFailure:
						_ = c.park(ctx)
					default:
						// nop
					}
				}
			}
		}
	}
}

func (p *Pool) collectConns() []*lazyConn {
	p.mtx.RLock()
	defer p.mtx.RUnlock()
	conns := make([]*lazyConn, 0, len(p.conns))
	for _, c := range p.conns {
		conns = append(conns, c)
	}

	return conns
}

func NewPool(ctx context.Context, config Config) *Pool {
	onDone := trace.DriverOnPoolNew(config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/conn.NewPool"),
	)
	defer onDone()

	p := &Pool{
		usages: 1,
		config: config,
		opts:   config.GrpcDialOptions(),
		conns:  make(map[endpoint.Key]*lazyConn),
		done:   make(chan struct{}),
	}

	if ttl := config.ConnectionTTL(); ttl > 0 {
		go p.connParker(xcontext.ValueOnly(ctx), ttl, ttl/2)
	}

	return p
}
