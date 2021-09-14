package ydb

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal"
	"github.com/ydb-platform/ydb-go-sdk/v3/timeutil"
)

type grpcClientStream struct {
	ctx    context.Context
	c      *conn
	d      *driver
	method Method
	s      grpc.ClientStream
	cancel context.CancelFunc
	recv   func(_ context.Context, address string, _ Method, _ error) func(_ context.Context, address string, _ Method, _ error)
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
	s.c.runtime.streamDone(timeutil.Now(), hideEOF(err))
	if s.done != nil {
		s.done(s.ctx, s.c.addr.String(), s.method, hideEOF(err))
	}
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
	s.c.runtime.streamRecv(timeutil.Now())

	err = s.s.RecvMsg(m)

	if err != nil {
		err = mapGRPCError(err)
		if te, ok := err.(*TransportError); ok && te.Reason != TransportErrorCanceled {
			// remove node from discovery cache on any transport error
			pessimizationDone := driverTraceOnPessimization(s.d.trace, s.ctx, s.c.Address(), err)
			pessimizationDone(s.ctx, s.c.Address(), s.d.cluster.Pessimize(s.c.addr))
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

	s.done = s.recv(s.Context(), s.c.Address(), s.method, hideEOF(err))

	return err
}
