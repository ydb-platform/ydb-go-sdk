package ydb

import (
	"context"
	"google.golang.org/grpc/metadata"
)

func (d *driver) Get(ctx context.Context) (_ ClientConnInterface, err error) {
	// Remember raw context to pass it for the tracing functions.
	rawCtx := ctx

	// Get credentials (token actually) for the request.
	var (
		md   metadata.MD
		conn *conn
	)
	md, err = d.meta.md(ctx)
	if err != nil {
		return
	}
	if len(md) > 0 {
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	driverTraceGetConnDone := driverTraceOnGetConn(ctx, d.trace, ctx)
	conn, err = d.cluster.Get(ctx)
	driverTraceGetConnDone(rawCtx, conn.Address(), err)

	if err != nil {
		return
	}

	return &grpcConn{
		conn: conn,
		d:    d,
	}, nil
}
