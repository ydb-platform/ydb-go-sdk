package driver

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/runtime/stats"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type driver struct {
	config.Config

	meta meta.Meta
	tls  bool

	clusterPessimize func(addr cluster.Addr) error
	clusterStats     func(it func(cluster.Endpoint, stats.Stats))
	clusterClose     func() error
	clusterGet       func(ctx context.Context) (conn conn.Conn, err error)
}

func (d *driver) Secure() bool {
	return d.tls
}

func (d *driver) Name() string {
	return d.Config.Database
}

func New(
	config config.Config,
	meta meta.Meta,
	tls bool,
	get func(ctx context.Context) (conn conn.Conn, err error),
	pessimize func(addr cluster.Addr) error,
	stats func(it func(cluster.Endpoint, stats.Stats)),
	close func() error,
) *driver {
	return &driver{
		Config:           config,
		meta:             meta,
		tls:              tls,
		clusterGet:       get,
		clusterPessimize: pessimize,
		clusterStats:     stats,
		clusterClose:     close,
	}
}

func (d *driver) RequestTimeout() time.Duration {
	return d.Config.RequestTimeout
}

func (d *driver) OperationTimeout() time.Duration {
	return d.Config.OperationTimeout
}

func (d *driver) OperationCancelAfter() time.Duration {
	return d.Config.OperationCancelAfter
}

func (d *driver) Meta(ctx context.Context) (context.Context, error) {
	return d.meta.Meta(ctx)
}

func (d *driver) Trace(ctx context.Context) trace.DriverTrace {
	return trace.ContextDriverTrace(ctx).Compose(d.Config.Trace)
}

func (d *driver) Pessimize(addr cluster.Addr) error {
	return d.clusterPessimize(addr)
}

func (d *driver) StreamTimeout() time.Duration {
	return d.Config.StreamTimeout
}

func (d *driver) KeepalivePolicy() *conn.KeepalivePolicy {
	return (*conn.KeepalivePolicy)(d.Config.KeepalivePolicy)
}
