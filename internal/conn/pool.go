package conn

import (
	"context"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xresolver"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type (
	connValue struct {
		cc       *conn
		useCount atomic.Int64
	}
	Pool struct {
		config      Config
		dialOptions []grpc.DialOption

		mu     sync.Mutex
		usages int64
		conns  map[endpoint.Key]*connValue
		closed bool
	}
)

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

func (p *Pool) newConnValue(e endpoint.Endpoint) (value *connValue) {
	defer func() {
		value.useCount.Add(1)
	}()

	return &connValue{
		cc: newConn(e, p),
	}
}

// Get returns a pooled connection wrapper for the endpoint and increments its use
// count. The gRPC connection is established lazily on the first Invoke or
// NewStream. Call [Pool.Put] when the endpoint is no longer needed.
func (p *Pool) Get(e endpoint.Endpoint) Conn {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	key := e.Key()

	if value, ok := p.conns[key]; ok {
		value.useCount.Add(1)
		value.cc.lastClusterAnnouncement.Store(time.Now().Unix())

		return value.cc
	}

	value := p.newConnValue(e)

	p.conns[key] = value

	return value.cc
}

// Put decrements the connection use count. When the count reaches zero, the gRPC
// connection is closed and the entry is removed from the pool.
func (p *Pool) Put(ctx context.Context, cc Conn) {
	c, ok := cc.(*conn)
	if !ok {
		return
	}

	key := c.endpoint.Key()

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}

	value, ok := p.conns[key]
	if !ok || value == nil || value.cc != c {
		return
	}

	if value.useCount.Add(-1) == 0 {
		delete(p.conns, key)
		_ = c.Close(ctx)
	}
}

func (p *Pool) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.closed
}

func (p *Pool) Ban(ctx context.Context, cc Conn, cause error) {
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()

	if closed {
		return
	}

	onDone := gtrace.DriverOnConnBan(
		p.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.(*Pool).Ban"),
		cc.Endpoint(), cc.State(), cause,
	)

	cc.Ban(ctx)

	onDone(cc.State())
}

func (p *Pool) Take(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.usages++

	return nil
}

func (p *Pool) Release(ctx context.Context) (finalErr error) {
	onDone := gtrace.DriverOnPoolRelease(p.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.(*Pool).Release"),
	)
	defer func() {
		onDone(finalErr)
	}()

	p.mu.Lock()
	p.usages--
	if p.usages > 0 {
		p.mu.Unlock()

		return nil
	}

	p.closed = true

	toClose := make([]closer.Closer, 0, len(p.conns))
	for _, value := range p.conns {
		toClose = append(toClose, value.cc)
	}

	p.conns = nil
	p.mu.Unlock()

	var (
		issues   []error
		issuesMu sync.Mutex
		wg       sync.WaitGroup
	)

	wg.Add(len(toClose))
	for _, c := range toClose {
		go func(c closer.Closer) {
			defer wg.Done()
			if err := c.Close(ctx); err != nil {
				issuesMu.Lock()
				issues = append(issues, err)
				issuesMu.Unlock()
			}
		}(c)
	}

	wg.Wait()

	if len(issues) > 0 {
		return xerrors.WithStackTrace(xerrors.NewWithIssues("connection pool close failed", issues...))
	}

	return nil
}

func NewPool(ctx context.Context, config Config) *Pool {
	onDone := gtrace.DriverOnPoolNew(config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.NewPool"),
	)
	defer onDone()

	p := &Pool{
		usages:      1,
		config:      config,
		dialOptions: config.GrpcDialOptions(),
		conns:       make(map[endpoint.Key]*connValue),
	}

	p.dialOptions = append(p.dialOptions,
		grpc.WithResolvers(
			xresolver.New("", gtrace.Compose(config.Trace(), &trace.Driver{
				OnResolve: func(info trace.DriverResolveStartInfo) func(trace.DriverResolveDoneInfo) {
					target := info.Target
					resolved := info.Resolved

					return func(info trace.DriverResolveDoneInfo) {
						if info.Error != nil || len(resolved) == 0 {
							p.mu.Lock()
							defer p.mu.Unlock()

							for key, value := range p.conns {
								cc := value.cc
								if u, err := url.Parse(key.Address); err == nil && u.Host == target && cc.grpcConn != nil {
									delete(p.conns, key)
									_ = cc.Close(xcontext.ValueOnly(ctx))
								}
							}
						}
					}
				},
			})),
		),
	)

	return p
}
