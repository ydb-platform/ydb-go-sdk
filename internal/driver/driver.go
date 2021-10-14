package driver

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type driver struct {
	config config.Config

	meta meta.Meta
	tls  bool

	clusterPessimize func(ctx context.Context, address string) error
	clusterClose     func(ctx context.Context) error
	clusterGet       func(ctx context.Context) (conn conn.Conn, err error)
}

func (d *driver) Secure() bool {
	return d.tls
}

func (d *driver) Name() string {
	return d.config.Database()
}

func New(
	config config.Config,
	meta meta.Meta,
	tls bool,
	get func(ctx context.Context) (conn conn.Conn, err error),
	pessimize func(ctx context.Context, address string) error,
	close func(ctx context.Context) error,
) *driver {
	return &driver{
		config:           config,
		meta:             meta,
		tls:              tls,
		clusterGet:       get,
		clusterPessimize: pessimize,
		clusterClose:     close,
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
	return d.clusterPessimize(ctx, address)
}

func (d *driver) StreamTimeout() time.Duration {
	return d.config.StreamTimeout()
}

func (d *driver) GrpcConnectionPolicy() conn.GrpcConnectionPolicy {
	return conn.GrpcConnectionPolicy(d.config.GrpcConnectionPolicy())
}
