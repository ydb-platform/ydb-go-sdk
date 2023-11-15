package conn

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type connsKey struct {
	address string
	nodeID  uint32
}

type Pool struct {
	usages int64
	config Config
	mtx    xsync.RWMutex
	opts   []grpc.DialOption
	conns  map[connsKey]*conn
	done   chan struct{}
}

func (p *Pool) Get(endpoint endpoint.Endpoint) Conn {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	var (
		address = endpoint.Address()
		cc      *conn
		has     bool
	)

	key := connsKey{address, endpoint.NodeID()}

	if cc, has = p.conns[key]; has {
		return cc
	}

	cc = newConn(
		endpoint,
		p.config,
		withOnClose(p.remove),
		withOnTransportError(p.Ban),
	)

	p.conns[key] = cc

	return cc
}

func (p *Pool) remove(c *conn) {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	delete(p.conns, connsKey{c.Endpoint().Address(), c.Endpoint().NodeID()})
}

func (p *Pool) isClosed() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

func (p *Pool) Ban(ctx context.Context, cc Conn, cause error) {
	if p.isClosed() {
		return
	}

	e := cc.Endpoint().Copy()

	p.mtx.RLock()
	defer p.mtx.RUnlock()

	cc, ok := p.conns[connsKey{e.Address(), e.NodeID()}]
	if !ok {
		return
	}

	trace.DriverOnConnBan(
		p.config.Trace(), &ctx,
		stack.FunctionID(""),
		e, cc.GetState(), cause,
	)(cc.SetState(ctx, Banned))
}

func (p *Pool) Allow(ctx context.Context, cc Conn) {
	if p.isClosed() {
		return
	}

	e := cc.Endpoint().Copy()

	p.mtx.RLock()
	defer p.mtx.RUnlock()

	cc, ok := p.conns[connsKey{e.Address(), e.NodeID()}]
	if !ok {
		return
	}

	trace.DriverOnConnAllow(
		p.config.Trace(), &ctx,
		stack.FunctionID(""),
		e, cc.GetState(),
	)(cc.Unban(ctx))
}

func (p *Pool) Take(context.Context) error {
	atomic.AddInt64(&p.usages, 1)
	return nil
}

func (p *Pool) Release(ctx context.Context) (finalErr error) {
	onDone := trace.DriverOnPoolRelease(p.config.Trace(), &ctx, stack.FunctionID(""))
	defer func() {
		onDone(finalErr)
	}()

	if atomic.AddInt64(&p.usages, -1) > 0 {
		return nil
	}

	close(p.done)

	var conns []closer.Closer
	p.mtx.WithRLock(func() {
		conns = make([]closer.Closer, 0, len(p.conns))
		for _, c := range p.conns {
			conns = append(conns, c)
		}
	})

	var (
		errCh = make(chan error, len(conns))
		wg    sync.WaitGroup
	)

	wg.Add(len(conns))
	for _, c := range conns {
		go func(c closer.Closer) {
			defer wg.Done()
			if err := c.Close(ctx); err != nil {
				errCh <- err
			}
		}(c)
	}
	wg.Wait()
	close(errCh)

	issues := make([]error, 0, len(conns))
	for err := range errCh {
		issues = append(issues, err)
	}

	if len(issues) > 0 {
		return xerrors.WithStackTrace(xerrors.NewWithIssues("connection pool close failed", issues...))
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

func (p *Pool) collectConns() []*conn {
	p.mtx.RLock()
	defer p.mtx.RUnlock()
	conns := make([]*conn, 0, len(p.conns))
	for _, c := range p.conns {
		conns = append(conns, c)
	}
	return conns
}

func NewPool(ctx context.Context, config Config) *Pool {
	onDone := trace.DriverOnPoolNew(config.Trace(), &ctx, stack.FunctionID(""))
	defer onDone()

	p := &Pool{
		usages: 1,
		config: config,
		opts:   config.GrpcDialOptions(),
		conns:  make(map[connsKey]*conn),
		done:   make(chan struct{}),
	}
	if ttl := config.ConnectionTTL(); ttl > 0 {
		go p.connParker(xcontext.WithoutDeadline(ctx), ttl, ttl/2)
	}
	return p
}
