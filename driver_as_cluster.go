package ydb

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func (d *driver) Invoke(ctx context.Context, method string, request interface{}, response interface{}, opts ...grpc.CallOption) (err error) {
	c, err := d.getConn(ctx)
	if err != nil {
		return
	}

	return c.Invoke(
		ctx,
		method,
		request,
		response,
		opts...,
	)
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

func (d *driver) Stats(it func(Endpoint, ConnStats)) {
	d.cluster.Stats(it)
}

func (d *driver) Close() error {
	return d.cluster.Close()
}

func (d *driver) getConn(ctx context.Context) (c *conn, err error) {
	// Remember raw context to pass it for the tracing functions.
	rawCtx := ctx

	// Get credentials (token actually) for the request.
	var md metadata.MD
	md, err = d.meta.md(ctx)
	if err != nil {
		return
	}
	if len(md) > 0 {
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	getConnDone := driverTraceOnGetConn(d.trace, ctx)
	c, err = d.cluster.Get(ctx)
	getConnDone(rawCtx, c.Address(), err)

	if err != nil {
		return nil, err
	}

	c = &conn{
		mtx:      c.mtx,
		grpcConn: c.grpcConn,
		dial:     c.dial,
		addr:     c.addr,
		driver:   d,
		runtime:  c.runtime,
		lifetime: c.lifetime,
		timer:    c.timer,
		done:     c.done,
	}
	if apply, ok := ContextClientConnApplier(rawCtx); ok {
		apply(c)
	}
	return c, err
}
