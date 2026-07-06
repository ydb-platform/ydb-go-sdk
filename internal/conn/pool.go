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
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Pool struct {
	usages      int64
	clock       clockwork.Clock
	config      Config
	dialOptions []grpc.DialOption
	conns       xsync.Map[endpoint.Key, *conn]
	done        chan struct{}
	// usageMu serializes useCount updates and the unused-connection cleanup pass.
	// A shared pool may serve several balancers; without the lock their discovery
	// repeaters can interleave ref changes with Close and map removal, closing a
	// freshly acquired connection or deleting a replacement *conn for the same key.
	// The mutex stays held across grpc Close in cleanup: releasing it earlier would
	// reopen that race. Discovery runs off the request path, so a bounded stall is
	// acceptable.
	usageMu sync.Mutex
}

func EndpointsToConnections(p *Pool, endpoints []endpoint.Endpoint) []Conn {
	conns := make([]Conn, 0, len(endpoints))
	for _, e := range endpoints {
		conns = append(conns, p.Get(e))
	}

	return conns
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

func (p *Pool) Get(endpoint endpoint.Endpoint) Conn {
	return p.conn(endpoint)
}

// conn returns the pooled *conn for the endpoint, creating one if needed.
// Split from [Pool.Get] so ref-count paths work with the concrete type without
// interface assertions; [Pool.Get] remains the public Conn-facing entry point.
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
//
// Use it when a connection must stay open for the whole driver lifetime and must
// not be closed by discovery cleanup — for example the bootstrap discovery client
// in [github.com/ydb-platform/ydb-go-sdk/v3.Driver.connect]. There is no matching
// release call: [Pool.Release] closes the entire pool on driver shutdown.
//
// Balancers should not call AcquireConn directly; they report endpoint diffs via
// [Pool.UpdateEndpointUsage] instead.
func (p *Pool) AcquireConn(e endpoint.Endpoint) Conn {
	p.usageMu.Lock()
	defer p.usageMu.Unlock()

	c := p.conn(e)
	c.useCount.Add(+1)

	return c
}

// UpdateEndpointUsage applies a batch of release/acquire marks and closes idle gRPC
// connections that nobody references anymore.
//
// Callers (typically a balancer) compute release and acquire from their own endpoint
// snapshot diff; the pool only tracks how many users hold each endpoint open and
// performs cleanup. Keeping diff logic in the caller avoids discovery-specific names
// and responsibilities in the pool API.
//
// Order is intentional: close connections with useCount <= 0 first, then release,
// then acquire. Endpoints dropped in this call still have useCount > 0 during the
// close pass and are removed at the start of the next call — see
// TestPool_EndpointUsage/DropsRemovedEndpointsOnNextDiscovery.
func (p *Pool) UpdateEndpointUsage(ctx context.Context, release, acquire []endpoint.Endpoint) {
	if p.isClosed() {
		return
	}

	p.usageMu.Lock()
	defer p.usageMu.Unlock()

	p.closeUnusedLocked(ctx)

	for _, e := range release {
		p.releaseUseCount(e)
	}
	for _, e := range acquire {
		p.acquireUseCount(e)
	}
}

// acquireUseCount marks the endpoint as held by one more pool user.
// Caller must hold usageMu; returns the connection so the caller can route traffic.
func (p *Pool) acquireUseCount(e endpoint.Endpoint) Conn {
	c := p.conn(e)
	c.useCount.Add(+1)

	return c
}

// releaseUseCount drops one use mark for the endpoint.
// Caller must hold usageMu. Missing map entries are ignored — the connection may
// already have been closed and removed by a prior cleanup pass.
func (p *Pool) releaseUseCount(e endpoint.Endpoint) {
	if cc, ok := p.conns.Get(e.Key()); ok {
		cc.useCount.Add(-1)
	}
}

// closeUnusedLocked closes every connection with useCount <= 0.
// Caller must hold usageMu. Closes run in parallel but wg.Wait blocks until all
// finish so the following ref updates see a stable map. grpc Close is bounded;
// we do not release usageMu before Wait because async cleanup would race with
// acquireUseCount and map replacement for the same endpoint key.
func (p *Pool) closeUnusedLocked(ctx context.Context) {
	var wg sync.WaitGroup

	p.conns.Range(func(_ endpoint.Key, c *conn) bool {
		if c.useCount.Load() <= 0 {
			wg.Add(1)
			go func(c closer.Closer) {
				defer wg.Done()
				_ = c.Close(ctx)
			}(c)
		}

		return true
	})
	wg.Wait()
}

// remove is called from conn onClose after grpc shutdown.
// Compare pointers, not keys: cleanup may already have inserted a new *conn for the
// same endpoint key; deleting by key alone would evict that replacement.
func (p *Pool) remove(c *conn) {
	if cc, ok := p.conns.Get(c.endpoint.Key()); ok && cc == c {
		p.conns.Delete(c.endpoint.Key())
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

	// Collect close errors without pre-sizing: conns map has no Len() (removed due to
	// incorrect size tracking); the final slice is small in practice.
	var (
		issues   []error
		issuesMu sync.Mutex
		wg       sync.WaitGroup
	)

	p.conns.Range(func(_ endpoint.Key, c *conn) bool {
		wg.Add(1)
		go func(c closer.Closer) {
			defer wg.Done()
			if err := c.Close(ctx); err != nil {
				issuesMu.Lock()
				issues = append(issues, err)
				issuesMu.Unlock()
			}
		}(c)

		return true
	})
	wg.Wait()

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
