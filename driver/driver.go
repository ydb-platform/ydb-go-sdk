package driver

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3/cluster/balancer/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/cluster/balancer/conn/addr"
	"github.com/ydb-platform/ydb-go-sdk/v3/cluster/balancer/conn/stats"
	"time"

	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type driver struct {
	clusterPessimize func(addr addr.Addr) error
	clusterStats     func(it func(endpoint.Endpoint, stats.Stats))
	clusterClose     func() error
	clusterGet       func(ctx context.Context) (conn conn.Conn, err error)

	meta  meta.Meta
	trace trace.DriverTrace

	requestTimeout       time.Duration
	streamTimeout        time.Duration
	operationTimeout     time.Duration
	operationCancelAfter time.Duration
}

func New(
	meta meta.Meta,
	trace trace.DriverTrace,
	requestTimeout time.Duration,
	streamTimeout time.Duration,
	operationTimeout time.Duration,
	operationCancelAfter time.Duration,
	get func(ctx context.Context) (conn conn.Conn, err error),
	pessimize func(addr addr.Addr) error,
	stats func(it func(endpoint.Endpoint, stats.Stats)),
	close func() error,
) conn.Driver {
	return &driver{
		meta:                 meta,
		trace:                trace,
		requestTimeout:       requestTimeout,
		streamTimeout:        streamTimeout,
		operationTimeout:     operationTimeout,
		operationCancelAfter: operationCancelAfter,
		clusterGet:           get,
		clusterPessimize:     pessimize,
		clusterStats:         stats,
		clusterClose:         close,
	}
}

func (d *driver) RequestTimeout() time.Duration {
	return d.requestTimeout
}

func (d *driver) OperationTimeout() time.Duration {
	return d.operationTimeout
}

func (d *driver) OperationCancelAfter() time.Duration {
	return d.operationCancelAfter
}

func (d *driver) Meta(ctx context.Context) (metadata.MD, error) {
	return d.meta.Meta(ctx)
}

func (d *driver) Trace(ctx context.Context) trace.DriverTrace {
	return trace.ContextDriverTrace(ctx).Compose(d.trace)
}

func (d *driver) Pessimize(addr addr.Addr) error {
	return d.clusterPessimize(addr)
}

func (d *driver) StreamTimeout() time.Duration {
	return d.streamTimeout
}
