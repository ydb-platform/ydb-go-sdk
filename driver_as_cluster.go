package ydb

import (
	"context"
	"google.golang.org/grpc"
)

func (d *driver) Invoke(ctx context.Context, method string, request interface{}, response interface{}, opts ...grpc.CallOption) (err error) {
	c, err := d.getConn(ctx)
	if err != nil {
		return
	}

	return c.Invoke(ctx, method, request, response, opts...)
}

func (d *driver) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (_ grpc.ClientStream, err error) {
	c, err := d.getConn(ctx)
	if err != nil {
		return
	}

	return c.raw.NewStream(ctx, desc, method, append(opts, grpc.MaxCallRecvMsgSize(50*1024*1024))...)
}

func (d *driver) Close() error {
	return d.cluster.Close()
}
