package driver

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/runtime/stats"
	cluster2 "github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/endpoint"
)

func (d *driver) Invoke(ctx context.Context, method string, request interface{}, response interface{}, opts ...grpc.CallOption) error {
	c, err := d.getConn(ctx)
	if err != nil {
		return err
	}

	e := c.Invoke(
		ctx,
		method,
		request,
		response,
		opts...,
	)

	fmt.Printf("%T %+v", e, e)

	return e
}

func (d *driver) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (_ grpc.ClientStream, err error) {
	c, err := d.getConn(ctx)
	if err != nil {
		return
	}

	return c.NewStream(
		ctx,
		desc,
		method,
		append(opts, grpc.MaxCallRecvMsgSize(50*1024*1024))...,
	)
}

func (d *driver) Stats() map[cluster2.Endpoint]stats.Stats {
	endpoints := make(map[cluster2.Endpoint]stats.Stats)
	m := sync.Mutex{}
	d.clusterStats(func(endpoint cluster2.Endpoint, s stats.Stats) {
		m.Lock()
		endpoints[endpoint] = s
		m.Unlock()
	})
	return endpoints
}

func (d *driver) Close() error {
	return d.clusterClose()
}

func (d *driver) getConn(ctx context.Context) (c conn.Conn, err error) {
	// Remember raw deadline to pass it for the tracing functions.
	rawCtx := ctx

	c, err = d.clusterGet(ctx)

	if err != nil {
		return nil, err
	}

	if apply, ok := cluster.ContextClientConnApplier(rawCtx); ok {
		apply(c)
	}

	return c, err
}
