package driver

import (
	"context"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn"
)

func (d *driver) Invoke(ctx context.Context, method string, request interface{}, response interface{}, opts ...grpc.CallOption) (err error) {
	var c conn.Conn
	c, err = d.getConn(ctx)
	if err != nil {
		return err
	}

	return c.Invoke(ctx, method, request, response, opts...)
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

func (d *driver) Close(ctx context.Context) error {
	return d.clusterClose(ctx)
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
