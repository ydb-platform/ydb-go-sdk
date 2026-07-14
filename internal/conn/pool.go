package conn

import (
	"context"
	"fmt"
	"net"
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
		useCount int64
	}
	Pool struct {
		config      Config
		dialOptions []grpc.DialOption

		mu     sync.Mutex
		usages int64
		conns  map[endpoint.Key]*connValue
		closed atomic.Bool
	}
)

var ErrClosedPool = xerrors.Wrap(fmt.Errorf("pool closed early"))

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

	if p.closed.Load() || p.conns == nil {
		return nil
	}

	cv := p.get(e)
	cv.useCount++

	return cv.cc
}

func (p *Pool) get(e endpoint.Endpoint) *connValue {
	if p.conns == nil {
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
func (p *Pool) Put(ctx context.Context, c Conn) {
	cc, ok := c.(*conn)
	if !ok || cc == nil {
		return
	}

	if !p.tryPut(cc) {
		_ = cc.Close(ctx)
	}
}

func (p *Pool) tryPut(c *conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed.Load() || p.conns == nil {
		return false
	}

	key := c.endpoint.Key()

	value, ok := p.conns[key]
	if !ok || value == nil || value.cc != c {
		return false
	}

	value.useCount--
	if value.useCount <= 0 {
		delete(p.conns, key)

		return false
	}

	return true
}

func (p *Pool) Ban(ctx context.Context, cc Conn, cause error) {
	if p.closed.Load() {
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

func (p *Pool) AddRef(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed.Load() {
		return xerrors.WithStackTrace(ErrClosedPool)
	}

	p.usages++

	return nil
}

func (p *Pool) RemoveRef(ctx context.Context) (finalErr error) {
	onDone := gtrace.DriverOnPoolRelease(p.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.(*Pool).RemoveRef"),
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

	if p.closed.Load() {
		return nil
	}

	p.usages--
	if p.usages > 0 {
		return nil
	}

	p.closed.Store(true)

	toClose := make([]closer.Closer, 0, len(p.conns))
	for _, value := range p.conns {
		toClose = append(toClose, value.cc)
	}

	p.conns = nil

	return toClose
}

func matchesResolveTarget(address, target string) bool {
	if address == target {
		return true
	}

	host, _, err := net.SplitHostPort(address)
	if err == nil && host == target {
		return true
	}

	return false
}

func (p *Pool) connsMatchingResolveTarget(target string) []*conn {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.conns == nil {
		return nil
	}

	result := make([]*conn, 0, len(p.conns))
	for key, value := range p.conns {
		if matchesResolveTarget(key.Address, target) {
			result = append(result, value.cc)
		}
	}

	return result
}

func (p *Pool) closeConnsForFailedResolve(ctx context.Context, target string) {
	toClose := p.connsMatchingResolveTarget(target)

	closeCtx := xcontext.ValueOnly(ctx)
	for _, cc := range toClose {
		cc.mtx.Lock()
		if !cc.closed {
			cc.close(closeCtx)
		}
		cc.mtx.Unlock()
	}
}

func (p *Pool) onResolveCallback(
	ctx context.Context,
	start trace.DriverResolveStartInfo,
) func(trace.DriverResolveDoneInfo) {
	target := start.Target
	resolved := start.Resolved

	return func(info trace.DriverResolveDoneInfo) {
		p.onResolveDone(ctx, target, resolved, info)
	}
}

func (p *Pool) onResolveDriverTrace(ctx context.Context, base *trace.Driver) *trace.Driver {
	return gtrace.Compose(base, &trace.Driver{
		OnResolve: func(info trace.DriverResolveStartInfo) func(trace.DriverResolveDoneInfo) {
			return p.onResolveCallback(ctx, info)
		},
	})
}

func (p *Pool) onResolveDone(ctx context.Context, target string, resolved []string, info trace.DriverResolveDoneInfo) {
	if info.Error != nil || len(resolved) == 0 {
		// Reset gRPC transport only; keep map entries and useCount unchanged.
		// The balancer still holds pool refs from discovery Get/Put; deleting
		// here would desync ref-counting and leak or double-close wrappers.
		// Stale entries are removed when discovery Put drops useCount to zero.
		p.closeConnsForFailedResolve(ctx, target)
	}
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
			xresolver.New("", p.onResolveDriverTrace(ctx, config.Trace())),
		),
	)

	return p
}
