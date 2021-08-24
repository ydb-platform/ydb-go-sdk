package ydb

import (
	"context"
	"github.com/YandexDatabase/ydb-go-genproto/protos/Ydb"
	"github.com/YandexDatabase/ydb-go-genproto/protos/Ydb_Issue"
	"github.com/YandexDatabase/ydb-go-sdk/v2/internal"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/YandexDatabase/ydb-go-sdk/v2/timeutil"
)

type grpcConn struct {
	c *conn
	d *driver
}

func (c *grpcConn) Address() string {
	if c.c != nil {
		return c.c.Address()
	}
	return ""
}

func (c *grpcConn) Invoke(ctx context.Context, method string, request interface{}, response interface{}, opts ...grpc.CallOption) (err error) {
	// Remember raw context to pass it for the tracing functions.
	rawCtx := ctx

	var (
		cancel context.CancelFunc
		opId   string
		issues []*Ydb_Issue.IssueMessage
	)
	if t := c.d.requestTimeout; t > 0 {
		ctx, cancel = context.WithTimeout(ctx, t)
	}
	defer func() {
		if cancel != nil {
			cancel()
		}
	}()
	if t := c.d.operationTimeout; t > 0 {
		ctx = WithOperationTimeout(ctx, t)
	}
	if t := c.d.operationCancelAfter; t > 0 {
		ctx = WithOperationCancelAfter(ctx, t)
	}

	// Get credentials (token actually) for the request.
	var md metadata.MD
	md, err = c.d.meta.md(ctx)
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

	cc := c.c
	if cc == nil {
		cc, err = c.d.getConn(ctx)
		if err != nil {
			return
		}
	}

	start := timeutil.Now()
	cc.runtime.operationStart(start)
	driverTraceOperationDone := driverTraceOnOperation(ctx, c.d.trace, ctx, cc.addr.String(), Method(method), params)
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
			driverTracePessimizationDone := driverTraceOnPessimization(ctx, c.d.trace, ctx, cc.addr.String(), err)
			driverTracePessimizationDone(ctx, cc.addr.String(), c.d.cluster.Pessimize(cc.addr))
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

func (c *grpcConn) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (_ grpc.ClientStream, err error) {
	// Remember raw context to pass it for the tracing functions.
	rawCtx := ctx

	var cancel context.CancelFunc
	if t := c.d.streamTimeout; t > 0 {
		ctx, cancel = context.WithTimeout(ctx, t)
		defer func() {
			if err != nil {
				cancel()
			}
		}()
	}

	// Get credentials (token actually) for the request.
	md, err := c.d.meta.md(ctx)
	if err != nil {
		return
	}
	if len(md) > 0 {
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	cc := c.c
	if cc == nil {
		cc, err = c.d.getConn(ctx)
		if err != nil {
			return
		}
	}

	cc.runtime.streamStart(timeutil.Now())
	driverTraceStreamDone := driverTraceOnStream(ctx, c.d.trace, ctx, cc.addr.String(), Method(method))
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
		c:      c,
		s:      s,
		cancel: cancel,
		done:   driverTraceStreamDone,
	}, nil
}
