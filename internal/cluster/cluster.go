package cluster

import (
	"context"
	"fmt"
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

// nolint: gofumpt
// nolint: nolintlint
var (
	// ErrClusterEmpty returned when no connections left in cluster.
	ErrClusterEmpty = xerrors.Wrap(fmt.Errorf("cluster empty"))
)

type Cluster struct {
	config config.Config
	pool   *conn.Pool
	conns  []conn.Conn

	balancer balancer.Balancer

	onBadStateCallback balancer.OnBadStateCallback
}

// Ban connection in underling pool
func (c *Cluster) Ban(ctx context.Context, cc conn.Conn, cause error) {
	c.pool.Ban(ctx, cc, cause)

	online := 0
	for _, cc := range c.conns {
		if cc.GetState() == conn.Online {
			online++
		}
	}

	if online*2 < len(c.conns) {
		c.onBadStateCallback(ctx)
	}
}

// Allow connection in underling pool
func (c *Cluster) Allow(ctx context.Context, cc conn.Conn) {
	c.pool.Allow(ctx, cc)
}

func New(
	ctx context.Context,
	config config.Config,
	pool *conn.Pool,
	endpoints []endpoint.Endpoint,
	onBadStateCallback balancer.OnBadStateCallback,
) *Cluster {
	onDone := trace.DriverOnClusterInit(config.Trace(), &ctx)
	defer func() {
		onDone(pool.Take(ctx))
	}()

	conns := make([]conn.Conn, 0, len(endpoints))
	for _, e := range endpoints {
		c := pool.Get(e)
		c.Unban()
		conns = append(conns, c)
	}

	balancer := multi.Balancer(
		// check conn from context at first place
		multi.WithBalancer(ctxbalancer.Balancer(conns), func(cc conn.Conn) bool {
			return true
		}),
		multi.WithBalancer(config.Balancer().Create(conns), func(cc conn.Conn) bool {
			return true
		}),
	)

	return &Cluster{
		config:             config,
		pool:               pool,
		conns:              conns,
		balancer:           balancer,
		onBadStateCallback: onBadStateCallback,
	}
}

func (c *Cluster) Close(ctx context.Context) (err error) {
	if c == nil {
		return nil
	}

	onDone := trace.DriverOnClusterClose(c.config.Trace(), &ctx)
	defer func() {
		onDone(err)
	}()

	return c.pool.Release(ctx)
}

func (c *Cluster) get(ctx context.Context) (cc conn.Conn, _ error) {
	for {
		select {
		case <-ctx.Done():
			return nil, xerrors.WithStackTrace(ctx.Err())

		default:
			cc = c.balancer.Next(ctx)

			if cc == nil {
				cc = c.balancer.Next(ctx, balancer.WithAcceptBanned(true))
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
