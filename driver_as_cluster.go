package ydb

import (
	"context"
	"github.com/YandexDatabase/ydb-go-genproto/protos/Ydb"
	"github.com/YandexDatabase/ydb-go-genproto/protos/Ydb_Issue"
	"github.com/YandexDatabase/ydb-go-sdk/v2/internal"
	"github.com/YandexDatabase/ydb-go-sdk/v2/timeutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func (d *driver) Invoke(ctx context.Context, method string, request interface{}, response interface{}, opts ...grpc.CallOption) (err error) {
	// Remember raw context to pass it for the tracing functions.
	rawCtx := ctx

	var (
		cancel context.CancelFunc
		opId   string
		issues []*Ydb_Issue.IssueMessage
	)
	if t := d.requestTimeout; t > 0 {
		ctx, cancel = context.WithTimeout(ctx, t)
	}
	defer func() {
		if cancel != nil {
			cancel()
		}
	}()
	if t := d.operationTimeout; t > 0 {
		ctx = WithOperationTimeout(ctx, t)
	}
	if t := d.operationCancelAfter; t > 0 {
		ctx = WithOperationCancelAfter(ctx, t)
	}

	// Get credentials (token actually) for the request.
	var md metadata.MD
	md, err = d.meta.md(ctx)
	if err != nil {
		return
	}
	if len(md) > 0 {
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	params := operationParams(ctx)
	if !params.Empty() {
		setOperationParams(request, params)
	}

	cc, err := d.getConn(ctx)
	if err != nil {
		return
	}

	start := timeutil.Now()
	cc.runtime.operationStart(start)
	driverTraceOperationDone := driverTraceOnOperation(ctx, d.trace, ctx, cc.addr.String(), Method(method), params)
	defer func() {
		driverTraceOperationDone(rawCtx, cc.addr.String(), Method(method), params, opId, issues, err)
		cc.runtime.operationDone(
			start, timeutil.Now(),
			errIf(isTimeoutError(err), err),
		)
	}()

	err = cc.raw.Invoke(ctx, method, request, response, opts...)

	if err != nil {
		err = mapGRPCError(err)
		if te, ok := err.(*TransportError); ok && te.Reason != TransportErrorCanceled {
			// remove node from discovery cache on any transport error
			driverTracePessimizationDone := driverTraceOnPessimization(ctx, d.trace, ctx, cc.addr.String(), err)
			driverTracePessimizationDone(ctx, cc.addr.String(), d.cluster.Pessimize(cc.addr))
		}
		return
	}
	if operation, ok := response.(internal.OpResponse); ok {
		opId = operation.GetOperation().GetId()
		issues = operation.GetOperation().GetIssues()
		switch {
		case !operation.GetOperation().GetReady():
			err = ErrOperationNotReady

		case operation.GetOperation().GetStatus() != Ydb.StatusIds_SUCCESS:
			err = &OpError{
				Reason: statusCode(operation.GetOperation().GetStatus()),
				issues: operation.GetOperation().GetIssues(),
			}
		}
	}

	return
}

func (d *driver) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (_ grpc.ClientStream, err error) {
	// Remember raw context to pass it for the tracing functions.
	rawCtx := ctx

	var cancel context.CancelFunc
	if t := d.streamTimeout; t > 0 {
		ctx, cancel = context.WithTimeout(ctx, t)
		defer func() {
			if err != nil {
				cancel()
			}
		}()
	}

	// Get credentials (token actually) for the request.
	md, err := d.meta.md(ctx)
	if err != nil {
		return
	}
	if len(md) > 0 {
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	cc, err := d.getConn(ctx)
	if err != nil {
		return
	}

	cc.runtime.streamStart(timeutil.Now())
	driverTraceStreamDone := driverTraceOnStream(ctx, d.trace, ctx, cc.addr.String(), Method(method))
	defer func() {
		if err != nil {
			cc.runtime.streamDone(timeutil.Now(), err)
			driverTraceStreamDone(rawCtx, cc.addr.String(), Method(method), err)
		}
	}()

	s, err := cc.raw.NewStream(ctx, desc, method, append(opts, grpc.MaxCallRecvMsgSize(50*1024*1024))...)
	if err != nil {
		return nil, mapGRPCError(err)
	}

	return &grpcClientStream{
		ctx:    rawCtx,
		c:      cc,
		d:      d,
		s:      s,
		cancel: cancel,
		done:   driverTraceStreamDone,
	}, nil
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
