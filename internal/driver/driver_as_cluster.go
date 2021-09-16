package driver

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/stats"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/endpoint"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
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

func (d *driver) Stats(it func(endpoint.Endpoint, stats.Stats)) {
	d.clusterStats(it)
}

func (d *driver) Close() error {
	return d.clusterClose()
}

func (d *driver) getConn(ctx context.Context) (c conn.Conn, err error) {
	// Remember raw context to pass it for the tracing functions.
	rawCtx := ctx

	// Get credentials (token actually) for the request.
	var md metadata.MD
	md, err = d.meta.Meta(ctx)
	if err != nil {
		return
	}
	if len(md) > 0 {
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	t := trace.ContextDriverTrace(ctx).Compose(d.trace)
	var getConnDone func(trace.GetConnDoneInfo)
	if t.OnGetConn != nil {
		getConnDone = t.OnGetConn(trace.GetConnStartInfo{
			Context: ctx,
		})
	}
	c, err = d.clusterGet(ctx)
	if getConnDone != nil {
		getConnDone(trace.GetConnDoneInfo{
			Address: c.Addr().String(),
			Error:   err,
		})
	}

	if err != nil {
		return nil, err
	}

	if apply, ok := ContextClientConnApplier(rawCtx); ok {
		apply(c)
	}
	c.SetDriver(d)
	return c, err
}
