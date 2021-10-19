package driver

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/cluster/stats"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type driver struct {
	config config.Config

	meta meta.Meta

	pessimize func(ctx context.Context, address string) error
	close     func(ctx context.Context) error
	stats     func(iterator func(address string, stats stats.Stats))
	get       func(ctx context.Context) (conn conn.Conn, err error)
}

func (d *driver) Stats(f func(address string, stats stats.Stats)) {
	d.stats(f)
}

func (d *driver) Secure() bool {
	return d.config.Secure()
}

func (d *driver) Name() string {
	return d.config.Database()
}

func New(
	config config.Config,
	meta meta.Meta,
	get func(ctx context.Context) (conn conn.Conn, err error),
	pessimize func(ctx context.Context, address string) error,
	stats func(iterator func(address string, stats stats.Stats)),
	close func(ctx context.Context) error,
) *driver {
	return &driver{
		config:    config,
		meta:      meta,
		get:       get,
		pessimize: pessimize,
		stats:     stats,
		close:     close,
	}
}

func (d *driver) RequestTimeout() time.Duration {
	return d.config.RequestTimeout()
}

func (d *driver) OperationTimeout() time.Duration {
	return d.config.OperationTimeout()
}

func (d *driver) OperationCancelAfter() time.Duration {
	return d.config.OperationCancelAfter()
}

func (d *driver) Meta(ctx context.Context) (context.Context, error) {
	return d.meta.Meta(ctx)
}

func (d *driver) Trace(ctx context.Context) trace.Driver {
	return trace.ContextDriver(ctx).Compose(d.config.Trace())
}

func (d *driver) Pessimize(ctx context.Context, address string) error {
	return d.pessimize(ctx, address)
}

func (d *driver) StreamTimeout() time.Duration {
	return d.config.StreamTimeout()
}

func (d *driver) GrpcConnectionPolicy() conn.GrpcConnectionPolicy {
	return conn.GrpcConnectionPolicy(d.config.GrpcConnectionPolicy())
}
