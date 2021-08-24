package ydb

import (
	"context"
	"google.golang.org/grpc/metadata"
)

func (d *driver) GetLazy() ClientConnInterface {
	return newGrpcConn(&multiConn{d: d}, d)
}

func (d *driver) Get(ctx context.Context) (_ ClientConnInterface, err error) {
	c, err := d.getConn(ctx)
	if err != nil {
		return nil, err
	}
	return newGrpcConn(&singleConn{c: c}, d), nil
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

	driverTraceGetConnDone := driverTraceOnGetConn(ctx, d.trace, ctx)
	c, err = d.cluster.Get(ctx)
	driverTraceGetConnDone(rawCtx, c.Address(), err)

	return
}
