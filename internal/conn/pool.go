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

type connWithCounter struct {
	counter atomic.Int32
	errs    atomic.Uint32
	conn    *conn
}

type Pool struct {
	usages int64
	config Config
	opts   []grpc.DialOption
	conns  xsync.Map[endpoint.Endpoint, *connWithCounter]
	done   chan struct{}
}

func (p *Pool) Put(ctx context.Context, c Conn) bool {
	if cc, has := p.conns.Get(c.Endpoint()); has {
		if cc.counter.Add(-1) == 0 {
			cc.conn.Close(ctx)
			p.conns.Delete(c.Endpoint())
		}

		return true
	}

	return false
}

func (p *Pool) Get(ctx context.Context, e endpoint.Endpoint) (Conn, error) {
	if cc, has := p.conns.Get(e); has {
		cc.counter.Add(1)

		return cc.conn, nil
	}

	cc := &connWithCounter{}

	cc.conn = dial(ctx, e,
		p.config,
		withOnTransportError(func(err error) {
			if IsBadConn(err) {
				cc.errs.Add(1)
			}
		}),
	)

	cc.counter.Add(1)

	p.conns.Set(e, cc)

	return cc.conn, nil
}

func (p *Pool) Take(_ context.Context) error {
	atomic.AddInt64(&p.usages, 1)

	return nil
}

func (p *Pool) Release(ctx context.Context) (finalErr error) {
	onDone := trace.DriverOnPoolRelease(p.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/conn.(*Pool).Release"),
	)
	defer func() {
		onDone(finalErr)
	}()

	if atomic.AddInt64(&p.usages, -1) > 0 {
		return nil
	}

	close(p.done)

	var conns []closer.Closer
	p.conns.Range(func(_ endpoint.Endpoint, cc *connWithCounter) bool {
		conns = append(conns, cc.conn)

		return true
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
			p.conns.Range(func(key endpoint.Endpoint, c *connWithCounter) bool {
				if time.Since(c.conn.LastUsage()) > ttl {
					switch c.conn.GetState() {
					case Online, Banned:
						_ = c.conn.park(ctx)
					default:
						// nop
					}
				}

				return true
			})
		}
	}
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
		done:   make(chan struct{}),
	}

	if ttl := config.ConnectionTTL(); ttl > 0 {
		go p.connParker(xcontext.ValueOnly(ctx), ttl, ttl/2) //nolint:gomnd
	}

	return p
}
