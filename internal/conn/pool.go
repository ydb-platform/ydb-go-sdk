package conn

import (
	"context"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jonboulle/clockwork"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xresolver"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xslices"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Pool struct {
	usages      int64
	clock       clockwork.Clock
	config      Config
	dialOptions []grpc.DialOption
	conns       xsync.Map[endpoint.Key, *conn]
	done        chan struct{}
	discoveryMu sync.Mutex
}

func (p *Pool) DialTimeout() time.Duration {
	return p.config.DialTimeout()
}

func (p *Pool) Trace() *trace.Driver {
	return p.config.Trace()
}

func (p *Pool) GrpcDialOptions() []grpc.DialOption {
	return p.dialOptions
}

func (p *Pool) get(endpoint endpoint.Endpoint) Conn {
	return p.conn(endpoint)
}

func (p *Pool) conn(endpoint endpoint.Endpoint) *conn {
	var (
		cc  *conn
		has bool
	)

	if cc, has = p.conns.Get(endpoint.Key()); has {
		return cc
	}

	cc = newConn(endpoint, p,
		withOnClose(p.remove),
		withTrackLastUsage(p.config.ConnectionTTL() > 0),
	)

	p.conns.Set(endpoint.Key(), cc)

	return cc
}

// AcquireConn returns a pooled connection and marks the endpoint as in use.
// Pair each call with [Pool.ReleaseEndpoint], or use [Pool.DiscoveryConnections] instead.
func (p *Pool) AcquireConn(e endpoint.Endpoint) Conn {
	p.discoveryMu.Lock()
	defer p.discoveryMu.Unlock()

	return p.acquireDiscoveryRef(e)
}

func (p *Pool) acquireDiscoveryRef(e endpoint.Endpoint) Conn {
	cc := p.conn(e)
	cc.discoveryRefs.Add(1)

	return cc
}

func (p *Pool) remove(c *conn) {
	p.conns.Delete(c.endpoint.Key())
}

// ReleaseEndpoint pairs with [Pool.AcquireConn].
// For discovery-driven updates use [Pool.DiscoveryConnections] instead.
func (p *Pool) ReleaseEndpoint(_ context.Context, e endpoint.Endpoint) {
	p.discoveryMu.Lock()
	defer p.discoveryMu.Unlock()

	p.releaseDiscoveryRef(e)
}

func (p *Pool) releaseDiscoveryRef(e endpoint.Endpoint) {
	if p.isClosed() {
		return
	}

	cc, ok := p.conns.Get(e.Key())
	if !ok {
		return
	}

	cc.discoveryRefs.Add(-1)
}

// closeUnreferencedEndpoints closes connections that are no longer in use.
func (p *Pool) closeUnreferencedEndpoints(ctx context.Context) {
	if p.isClosed() {
		return
	}

	p.conns.Range(func(_ endpoint.Key, c *conn) bool {
		if c.discoveryRefs.Load() <= 0 {
			_ = c.Close(ctx)
		}

		return true
	})
}

// DiscoveryConnections is the preferred API for discovery-driven pool updates.
// Alternatively pair [Pool.AcquireConn] with [Pool.ReleaseEndpoint].
func (p *Pool) DiscoveryConnections(
	ctx context.Context,
	added, dropped, newest []endpoint.Endpoint,
) []Conn {
	if p.isClosed() {
		return nil
	}

	p.discoveryMu.Lock()
	defer p.discoveryMu.Unlock()

	p.closeUnreferencedEndpoints(ctx)

	for _, e := range dropped {
		p.releaseDiscoveryRef(e)
	}

	for _, e := range added {
		p.acquireDiscoveryRef(e)
	}

	return xslices.Transform(newest, p.get)
}

// ReleaseEndpoints is a batch [Pool.ReleaseEndpoint] (part of the [Pool.AcquireConn] lifecycle).
func (p *Pool) ReleaseEndpoints(ctx context.Context, endpoints []endpoint.Endpoint) {
	p.discoveryMu.Lock()
	defer p.discoveryMu.Unlock()

	for _, e := range endpoints {
		p.releaseDiscoveryRef(e)
	}
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

	gtrace.DriverOnConnBan(
		p.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.(*Pool).Ban"),
		cc.Endpoint().Copy(), cc.GetState(), cause,
	)(cc.SetState(ctx, state.Banned))
}

func (p *Pool) Allow(ctx context.Context, cc Conn) {
	if p.isClosed() {
		return
	}

	e := cc.Endpoint().Copy()

	cc, ok := p.conns.Get(e.Key())
	if !ok {
		return
	}

	gtrace.DriverOnConnAllow(
		p.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.(*Pool).Allow"),
		e, cc.GetState(),
	)(cc.Unban(ctx))
}

func (p *Pool) Take(context.Context) error {
	atomic.AddInt64(&p.usages, 1)

	return nil
}

func (p *Pool) Release(ctx context.Context) (finalErr error) {
	onDone := gtrace.DriverOnPoolRelease(p.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.(*Pool).Release"),
	)
	defer func() {
		onDone(finalErr)
	}()

	if atomic.AddInt64(&p.usages, -1) > 0 {
		return nil
	}

	close(p.done)

	var (
		errCh = make(chan error, p.conns.Len())
		wg    sync.WaitGroup
	)

	wg.Add(cap(errCh))
	p.conns.Range(func(_ endpoint.Key, c *conn) bool {
		go func(c closer.Closer) {
			defer wg.Done()
			if err := c.Close(ctx); err != nil {
				errCh <- err
			}
		}(c)

		return true
	})
	wg.Wait()
	close(errCh)

	issues := make([]error, 0, cap(errCh))
	for err := range errCh {
		issues = append(issues, err)
	}

	if len(issues) > 0 {
		return xerrors.WithStackTrace(xerrors.NewWithIssues("connection pool close failed", issues...))
	}

	return nil
}

func (p *Pool) connParker(ctx context.Context, ttl, interval time.Duration) {
	ticker := p.clock.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-p.done:
			return
		case <-ticker.Chan():
			p.conns.Range(func(_ endpoint.Key, c *conn) bool {
				if t, err := c.LastUsage(); err == nil && time.Since(*t) > ttl {
					switch c.GetState() {
					case state.Online, state.Banned:
						_ = c.park(ctx)
					default:
						// nop
					}
				}

				return true
			})
		}
	}
}

type poolOption func(p *Pool)

func NewPool(ctx context.Context, config Config, opts ...poolOption) *Pool {
	onDone := gtrace.DriverOnPoolNew(config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.NewPool"),
	)
	defer onDone()

	p := &Pool{
		usages:      1,
		clock:       clockwork.NewRealClock(),
		config:      config,
		dialOptions: config.GrpcDialOptions(),
		done:        make(chan struct{}),
	}

	for _, opt := range opts {
		opt(p)
	}

	p.dialOptions = append(p.dialOptions,
		grpc.WithResolvers(
			xresolver.New("", gtrace.Compose(config.Trace(), &trace.Driver{
				OnResolve: func(info trace.DriverResolveStartInfo) func(trace.DriverResolveDoneInfo) {
					target := info.Target
					resolved := info.Resolved

					return func(info trace.DriverResolveDoneInfo) {
						if info.Error != nil || len(resolved) == 0 {
							p.conns.Range(func(key endpoint.Key, cc *conn) bool {
								if u, err := url.Parse(key.Address); err == nil && u.Host == target && cc.grpcConn != nil {
									_ = cc.grpcConn.Close()
									_ = p.conns.Delete(key)
								}

								return true
							})
						}
					}
				},
			})),
		),
	)

	if ttl := config.ConnectionTTL(); ttl > 0 {
		go p.connParker(xcontext.ValueOnly(ctx), ttl, ttl/2) //nolint:mnd
	}

	return p
}
