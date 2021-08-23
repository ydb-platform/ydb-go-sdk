package ydb

import (
	"context"
	"github.com/YandexDatabase/ydb-go-sdk/v2/internal"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/YandexDatabase/ydb-go-genproto/protos/Ydb"
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
	err = s.CloseSend()
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
	return mapGRPCError(s.s.SendMsg(m))
}

func (s *grpcClientStream) RecvMsg(m interface{}) (err error) {
	s.c.conn.runtime.streamRecv(timeutil.Now())
	driverTraceStreamRecvDone := driverTraceOnStreamRecv(s.ctx, s.c.d.trace, s.Context(), s.c.conn.addr.String(), s.method)

	err = s.RecvMsg(m)
	if err != nil {
		err = mapGRPCError(err)
	} else {
		operationResponse := m.(internal.OpResponse)
		if operationResponse != nil {
			driverTraceStreamRecvDone(s.ctx, s.c.conn.addr.String(), s.method, operationResponse.GetOperation().GetIssues(), hideEOF(err))
		} else {
			driverTraceStreamRecvDone(s.ctx, s.c.conn.addr.String(), s.method, nil, hideEOF(err))
		}
		if s := operationResponse.GetOperation().GetStatus(); s != Ydb.StatusIds_SUCCESS {
			err = &OpError{
				Reason: statusCode(s),
				issues: operationResponse.GetOperation().GetIssues(),
			}
		}
	}
	return err
}
