package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/ctxbalancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/multi"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

const (
	MaxGetConnTimeout = 10 * time.Second
)

var (
	// ErrClusterClosed returned when requested on a closed cluster.
	ErrClusterClosed = xerrors.Wrap(fmt.Errorf("cluster closed"))

	// ErrClusterEmpty returned when no connections left in cluster.
	ErrClusterEmpty = xerrors.Wrap(fmt.Errorf("cluster empty"))
)

type Cluster struct {
	config                config.Config
	pool                  conn.Pool
	conns                 []conn.Conn
	balancerPointer       balancer.Balancer
	needDiscoveryCallback balancer.OnBadStateCallback

	m    sync.RWMutex
	done chan struct{}
}

func (c *Cluster) isClosed() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

// Pessimize connection in underling pool
func (c *Cluster) Pessimize(ctx context.Context, cc conn.Conn, cause error) {
	c.pool.Pessimize(ctx, cc, cause)
}

// Unpessimize connection in underling pool
func (c *Cluster) Unpessimize(ctx context.Context, cc conn.Conn) {
	c.pool.Unpessimize(ctx, cc)
}

func New(
	ctx context.Context,
	config config.Config,
	pool conn.Pool,
	endpoints []endpoint.Endpoint,
	needDiscoveryCallback balancer.OnBadStateCallback,
) *Cluster {
	onDone := trace.DriverOnClusterInit(config.Trace(), &ctx)
	defer func() {
		onDone(pool.Take(ctx))
	}()

	conns := make([]conn.Conn, 0, len(endpoints))
	for _, e := range endpoints {
		conns = append(conns, pool.Get(e))
	}

	parkBanned(ctx, conns)

	clusterBalancer := multi.Balancer(
		// check conn from context at first place
		multi.WithBalancer(ctxbalancer.Balancer(conns), func(cc conn.Conn) bool {
			return true
		}),
		multi.WithBalancer(config.Balancer().Create(conns), func(cc conn.Conn) bool {
			return true
		}),
	)

	return &Cluster{
		done:                  make(chan struct{}),
		config:                config,
		pool:                  pool,
		balancerPointer:       clusterBalancer,
		conns:                 conns,
		needDiscoveryCallback: needDiscoveryCallback,
	}
}

func (c *Cluster) Close(ctx context.Context) (err error) {
	close(c.done)

	onDone := trace.DriverOnClusterClose(c.config.Trace(), &ctx)
	defer func() {
		onDone(err)
	}()

	c.m.Lock()
	defer c.m.Unlock()

	var issues []error

	for _, cc := range c.conns {
		if err := cc.Release(ctx); err != nil {
			issues = append(issues, err)
		}
	}

	if len(issues) > 0 {
		return xerrors.WithStackTrace(xerrors.NewWithIssues("cluster closed with issues", issues...))
	}

	return nil
}

func (c *Cluster) balancer() balancer.Balancer {
	c.m.RLock()
	defer c.m.RUnlock()

	return c.balancerPointer
}

func (c *Cluster) get(ctx context.Context) (cc conn.Conn, _ error) {
	for {
		select {
		case <-c.done:
			return nil, xerrors.WithStackTrace(ErrClusterClosed)
		case <-ctx.Done():
			return nil, xerrors.WithStackTrace(ctx.Err())
		default:
			cc = c.balancer().Next(ctx, balancer.WithOnBadState(c.needDiscoveryCallback))

			if cc == nil {
				cc = c.balancer().Next(ctx, balancer.WithAcceptBanned(true))
			}

			if cc == nil {
				err := ErrClusterEmpty
				if ctxErr := ctx.Err(); ctxErr != nil {
					err = ctxErr
				}
				return nil, xerrors.WithStackTrace(err)
			}
			if err := cc.Ping(ctx); err == nil {
				return cc, nil
			}
		}
	}
}

// Get returns next available connection.
// It returns error on given deadline cancellation or when cluster become closed.
func (c *Cluster) Get(ctx context.Context) (cc conn.Conn, err error) {
	if c.isClosed() {
		return nil, xerrors.WithStackTrace(ErrClusterClosed)
	}

	var cancel context.CancelFunc
	// without client context deadline lock limited on MaxGetConnTimeout
	// cluster endpoints cannot be updated at this time
	ctx, cancel = context.WithTimeout(ctx, MaxGetConnTimeout)
	defer cancel()

	onDone := trace.DriverOnClusterGet(c.config.Trace(), &ctx)
	defer func() {
		if err != nil {
			onDone(nil, err)
		} else {
			onDone(cc.Endpoint().Copy(), nil)
		}
	}()

	return c.get(ctx)
}

func parkBanned(ctx context.Context, conns []conn.Conn) {
	for _, c := range conns {
		if c.GetState() == conn.Banned {
			_ = c.Park(ctx)
		}
	}
}
