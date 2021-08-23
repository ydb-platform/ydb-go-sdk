package ydb

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func (d *driver) Get(ctx context.Context) (_ grpc.ClientConnInterface, err error) {
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

	conn, backoffUseBalancer := ContextConn(rawCtx)
	if backoffUseBalancer && (conn == nil || conn.runtime.getState() != ConnOnline) {
		driverTraceGetConnDone := driverTraceOnGetConn(ctx, d.trace, ctx)
		conn, err = d.cluster.Get(ctx)
		addr := ""
		if conn != nil {
			addr = conn.addr.String()
		}
		driverTraceGetConnDone(rawCtx, addr, err)
		if err != nil {
			return
		}
	}

	return &grpcConn{
		conn: conn,
		d:    d,
	}, nil
}
