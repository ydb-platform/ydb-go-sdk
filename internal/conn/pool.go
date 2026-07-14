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

func endpointsToConnections(p *Pool, endpoints []endpoint.Endpoint) []Conn {
	p.mu.Lock()
	defer p.mu.Unlock()

	conns := make([]Conn, 0, len(endpoints))
	for _, e := range endpoints {
		cv := p.get(e)
		if cv != nil {
			conns = append(conns, cv.cc)
		}
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

// Get returns a pooled connection wrapper for the endpoint and increments its use
// count. The gRPC connection is established lazily on the first Invoke or
// NewStream. Call [Pool.Put] when the endpoint is no longer needed.
func (p *Pool) Get(e endpoint.Endpoint) Conn {
	p.mu.Lock()
	defer p.mu.Unlock()

	cv := p.get(e)
	if cv == nil {
		return nil
	}

	cv.useCount.Add(1)

	return cv.cc
}

func (p *Pool) get(e endpoint.Endpoint) *connValue {
	if p.closed {
		return nil
	}

	key := e.Key()

	if value, ok := p.conns[key]; ok {
		value.cc.lastClusterAnnouncement.Store(time.Now().Unix())

		return value
	}

	value := &connValue{
		cc: newConn(e, p),
	}

	p.conns[key] = value

	return value
}

// Put decrements the connection use count. When the count reaches zero, the gRPC
// connection is closed and the entry is removed from the pool.
func (p *Pool) Put(ctx context.Context, cc Conn) {
	c, ok := cc.(*conn)
	if !ok {
		return
	}

	if !p.tryPut(c) {
		_ = c.Close(ctx)
	}
}

func (p *Pool) tryPut(c *conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return false
	}

	key := c.endpoint.Key()

	value, ok := p.conns[key]
	if !ok || value == nil || value.cc != c {
		return false
	}

	if value.useCount.Add(-1) == 0 {
		delete(p.conns, key)

		return false
	}
	
	return true
}

func (p *Pool) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.closed
}

func (p *Pool) Ban(ctx context.Context, cc Conn, cause error) {
	if p.isClosed() {
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

	toClose := p.release()
	if len(toClose) == 0 {
		return nil
	}

	var (
		issues   []error
		issuesMu sync.Mutex
		wg       sync.WaitGroup
	)
	wg.Add(len(toClose))
	for _, c := range toClose {
		go func(c closer.Closer) {
			defer wg.Done()
			if err := c.Close(ctx); err != nil && !xerrors.IsContextError(err) {
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

func (p *Pool) release() []closer.Closer {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.usages--
	if p.usages > 0 {
		return nil
	}

	p.closed = true

	toClose := make([]closer.Closer, 0, len(p.conns))
	for _, value := range p.conns {
		toClose = append(toClose, value.cc)
	}

	p.conns = nil

	return toClose
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
								if u, err := url.Parse(key.Address); err == nil && u.Host == target {
									value.cc.mtx.Lock()
									value.cc.close(ctx)
									value.cc.mtx.Unlock()
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
