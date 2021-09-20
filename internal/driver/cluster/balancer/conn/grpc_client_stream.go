package conn

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/timeutil"
)

type grpcClientStream struct {
	ctx    context.Context
	c      *conn
	d      *Config
	method trace.Method
	s      grpc.ClientStream
	cancel context.CancelFunc
	recv   func(trace.StreamRecvDoneInfo) func(trace.StreamDoneInfo)
	done   func(trace.StreamDoneInfo)
}

func (s *grpcClientStream) Header() (metadata.MD, error) {
	return s.s.Header()
}

func (s *grpcClientStream) Trailer() metadata.MD {
	return s.s.Trailer()
}

func (s *grpcClientStream) CloseSend() (err error) {
	err = s.s.CloseSend()
	if err != nil {
		err = errors.MapGRPCError(err)
	}
	s.c.runtime.StreamDone(timeutil.Now(), errors.HideEOF(err))
	if s.done != nil {
		s.done(trace.StreamDoneInfo{
			Error: errors.HideEOF(err),
		})
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
		err = errors.MapGRPCError(err)
	}
	return
}

func (s *grpcClientStream) RecvMsg(m interface{}) (err error) {
	s.c.runtime.StreamRecv(timeutil.Now())

	err = s.s.RecvMsg(m)

	if err != nil {
		err = errors.MapGRPCError(err)
		if errors.MustPessimizeEndpoint(err) {
			s.c.pessimize(s.ctx, err)
		}
		return
	}

	if operation, ok := m.(internal.StreamOperationResponse); ok {
		if s := operation.GetStatus(); s != Ydb.StatusIds_SUCCESS {
			err = errors.NewOpError(errors.WithOEOperation(operation))
		}
	}

	if s.recv != nil {
		s.done = s.recv(trace.StreamRecvDoneInfo{
			Error: errors.HideEOF(err),
		})
	}

	return err
}
