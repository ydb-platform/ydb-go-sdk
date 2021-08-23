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

type grpcClientStream struct {
	ctx    context.Context
	c      *grpcConn
	method Method
	s      grpc.ClientStream
	cancel context.CancelFunc
	done   func(_ context.Context, address string, _ Method, _ error)
}

func (s *grpcClientStream) Header() (metadata.MD, error) {
	return s.Header()
}

func (s *grpcClientStream) Trailer() metadata.MD {
	return s.Trailer()
}

func (s *grpcClientStream) CloseSend() (err error) {
	err = s.s.CloseSend()
	if err != nil {
		err = mapGRPCError(err)
	}
	s.c.conn.runtime.streamDone(timeutil.Now(), hideEOF(err))
	s.done(s.ctx, s.c.conn.addr.String(), s.method, hideEOF(err))
	if s.cancel != nil {
		s.cancel()
	}
	return err
}

func (s *grpcClientStream) Context() context.Context {
	return s.s.Context()
}

func (s *grpcClientStream) SendMsg(m interface{}) (err error) {
	err = s.s.SendMsg(m)
	if err != nil {
		err = mapGRPCError(err)
	}
	return
}

func (s *grpcClientStream) RecvMsg(m interface{}) (err error) {
	var (
		issues []*Ydb_Issue.IssueMessage
	)

	s.c.conn.runtime.streamRecv(timeutil.Now())

	driverTraceStreamRecvDone := driverTraceOnStreamRecv(s.ctx, s.c.d.trace, s.Context(), s.c.Address(), s.method)
	defer func() {
		driverTraceStreamRecvDone(s.ctx, s.c.conn.addr.String(), s.method, issues, hideEOF(err))
	}()

	err = s.s.RecvMsg(m)

	if err != nil {
		err = mapGRPCError(err)
		if te, ok := err.(*TransportError); ok && te.Reason != TransportErrorCanceled {
			// remove node from discovery cache on any transport error
			driverTracePessimizationDone := driverTraceOnPessimization(s.ctx, s.c.d.trace, s.ctx, s.c.Address(), err)
			driverTracePessimizationDone(s.ctx, s.c.Address(), s.c.d.cluster.Pessimize(s.c.conn.addr))
		}
		return
	}

	if operation, ok := m.(internal.StreamOperationResponse); ok {
		if s := operation.GetStatus(); s != Ydb.StatusIds_SUCCESS {
			err = &OpError{
				Reason: statusCode(s),
				issues: operation.GetIssues(),
			}
		}
	}

	return err
}
