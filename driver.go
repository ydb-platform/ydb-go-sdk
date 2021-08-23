package ydb

import (
	"context"
	"path"
	"time"

	"github.com/YandexDatabase/ydb-go-genproto/protos/Ydb"
	"github.com/YandexDatabase/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/YandexDatabase/ydb-go-sdk/v2/internal"
	"github.com/YandexDatabase/ydb-go-sdk/v2/timeutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type driver struct {
	cluster *cluster
	meta    *meta
	trace   DriverTrace

	requestTimeout       time.Duration
	streamTimeout        time.Duration
	operationTimeout     time.Duration
	operationCancelAfter time.Duration
}

func (d *driver) Close() error {
	return d.cluster.Close()
}

func (d *driver) Call(ctx context.Context, op Operation) (info CallInfo, err error) {
	// Remember raw context to pass it for the tracing functions.
	rawCtx := ctx

	if t := d.requestTimeout; t > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t)
		defer cancel()
	}
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

	info = &callInfo{
		conn: conn,
	}

	if conn.raw == nil {
		return info, ErrNilConnection
	}

	method, req, res, resp := internal.Unwrap(op)
	if resp == nil {
		resp = internal.WrapOpResponse(&Ydb_Operations.GetOperationResponse{})
	}

	params := operationParams(ctx)
	if !params.Empty() {
		setOperationParams(req, params)
	}

	start := timeutil.Now()
	conn.runtime.operationStart(start)
	driverTraceOperationDone := driverTraceOnOperation(ctx, d.trace, ctx, conn.addr.String(), Method(method), params)

	err = invoke(ctx, conn.raw, resp, method, req, res)

	conn.runtime.operationDone(
		start, timeutil.Now(),
		errIf(isTimeoutError(err), err),
	)
	driverTraceOperationDone(rawCtx, conn.addr.String(), Method(method), params, resp.GetOpID(), resp.GetIssues(), err)

	if err != nil {
		if te, ok := err.(*TransportError); ok && te.Reason != TransportErrorCanceled {
			// remove node from discovery cache on any transport error
			driverTracePessimizationDone := driverTraceOnPessimization(ctx, d.trace, ctx, conn.addr.String(), err)
			driverTracePessimizationDone(rawCtx, conn.addr.String(), d.cluster.Pessimize(conn.addr))
		}
	}

	return
}

func (d *driver) StreamRead(ctx context.Context, op StreamOperation) (info CallInfo, err error) {
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

	conn, backoffUseBalancer := ContextConn(rawCtx)
	if backoffUseBalancer && (conn == nil || conn.runtime.getState() != ConnOnline) {
		driverTraceGetConnDone := driverTraceOnGetConn(ctx, d.trace, ctx)
		conn, err = d.cluster.Get(ctx)
		addr := ""
		if conn != nil {
			addr = conn.addr.String()
		}
		driverTraceGetConnDone(rawCtx, addr, err)
	}

	info = &callInfo{
		conn: conn,
	}

	if err != nil {
		return
	}

	method, req, resp, process := internal.UnwrapStreamOperation(op)
	desc := grpc.StreamDesc{
		StreamName:    path.Base(method),
		ServerStreams: true,
	}

	conn.runtime.streamStart(timeutil.Now())
	driverTraceStreamDone := driverTraceOnStream(ctx, d.trace, ctx, conn.addr.String(), Method(method))
	defer func() {
		if err != nil {
			conn.runtime.streamDone(timeutil.Now(), err)
			driverTraceStreamDone(rawCtx, conn.addr.String(), Method(method), err)
		}
	}()

	s, err := grpc.NewClientStream(ctx, &desc, conn.raw, method,
		grpc.MaxCallRecvMsgSize(50*1024*1024), // 50MB
	)
	if err != nil {
		return info, mapGRPCError(err)
	}
	if err := s.SendMsg(req); err != nil {
		return info, mapGRPCError(err)
	}
	if err := s.CloseSend(); err != nil {
		return info, mapGRPCError(err)
	}

	go func() {
		var err error
		defer func() {
			conn.runtime.streamDone(timeutil.Now(), hideEOF(err))
			driverTraceStreamDone(rawCtx, conn.addr.String(), Method(method), hideEOF(err))
			if cancel != nil {
				cancel()
			}
		}()
		for err == nil {
			conn.runtime.streamRecv(timeutil.Now())
			driverTraceStreamRecvDone := driverTraceOnStreamRecv(ctx, d.trace, ctx, conn.addr.String(), Method(method))

			err = s.RecvMsg(resp)
			if resp != nil {
				driverTraceStreamRecvDone(rawCtx, conn.addr.String(), Method(method), resp.GetIssues(), hideEOF(err))
			} else {
				driverTraceStreamRecvDone(rawCtx, conn.addr.String(), Method(method), nil, hideEOF(err))
			}
			if err != nil {
				err = mapGRPCError(err)
			} else {
				if s := resp.GetStatus(); s != Ydb.StatusIds_SUCCESS {
					err = &OpError{
						Reason: statusCode(s),
						issues: resp.GetIssues(),
					}
				}
			}
			// NOTE: do not hide even io.EOF for this call.
			process(err)
		}
	}()

	return info, nil
}
