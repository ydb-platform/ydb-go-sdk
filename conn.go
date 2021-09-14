package ydb

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stats"
	"github.com/ydb-platform/ydb-go-sdk/v3/timeutil"
)

type conn struct {
	raw  *grpc.ClientConn
	addr connAddr

	runtime connRuntime
}

func (c *conn) Invoke(ctx context.Context, method string, request interface{}, response interface{}, opts ...grpc.CallOption) (err error) {
	// Remember raw context to pass it for the tracing functions.
	rawCtx := ctx
	c.raw.Target()

	var (
		cancel context.CancelFunc
		opId   string
		issues []*Ydb_Issue.IssueMessage

		d = contextDriver(ctx)
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

	start := timeutil.Now()
	c.runtime.operationStart(start)
	driverTraceOperationDone := driverTraceOnOperation(ctx, d.trace, ctx, c.Address(), Method(method), params)
	defer func() {
		driverTraceOperationDone(rawCtx, c.Address(), Method(method), params, opId, issues, err)
		c.runtime.operationDone(
			start, timeutil.Now(),
			errIf(isTimeoutError(err), err),
		)
	}()

	err = c.raw.Invoke(ctx, method, request, response, opts...)

	if err != nil {
		err = mapGRPCError(err)
		if te, ok := err.(*TransportError); ok && te.Reason != TransportErrorCanceled {
			// remove node from discovery cache on any transport error
			driverTracePessimizationDone := driverTraceOnPessimization(ctx, d.trace, ctx, c.Address(), err)
			driverTracePessimizationDone(ctx, c.Address(), d.cluster.Pessimize(c.addr))
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

func (c *conn) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (_ grpc.ClientStream, err error) {
	// Remember raw context to pass it for the tracing functions.
	rawCtx := ctx

	var (
		cancel context.CancelFunc

		d = contextDriver(ctx)
	)
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

	c.runtime.streamStart(timeutil.Now())
	driverTraceStreamDone := driverTraceOnStream(ctx, d.trace, ctx, c.Address(), Method(method))
	defer func() {
		if err != nil {
			c.runtime.streamDone(timeutil.Now(), err)
			driverTraceStreamDone(rawCtx, c.Address(), Method(method), err)
		}
	}()

	s, err := c.raw.NewStream(ctx, desc, method, append(opts, grpc.MaxCallRecvMsgSize(50*1024*1024))...)
	if err != nil {
		return nil, mapGRPCError(err)
	}

	return &grpcClientStream{
		ctx:    rawCtx,
		c:      c,
		d:      d,
		s:      s,
		cancel: cancel,
		done:   driverTraceStreamDone,
	}, nil
}

func (c *conn) Address() string {
	if c != nil {
		return c.addr.String()
	}
	return ""
}

func newConn(cc *grpc.ClientConn, addr connAddr) *conn {
	const (
		statsDuration = time.Minute
		statsBuckets  = 12
	)
	return &conn{
		raw:  cc,
		addr: addr,
		runtime: connRuntime{
			opTime:  stats.NewSeries(statsDuration, statsBuckets),
			opRate:  stats.NewSeries(statsDuration, statsBuckets),
			errRate: stats.NewSeries(statsDuration, statsBuckets),
		},
	}
}
