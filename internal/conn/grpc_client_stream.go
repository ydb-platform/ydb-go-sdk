package conn

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/wrap"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type grpcClientStream struct {
	c        *conn
	s        grpc.ClientStream
	wrapping bool
	onDone   func(ctx context.Context)
	recv     func(error) func(trace.ConnState, error)
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
		err = errors.Error(errors.MapGRPCError(err))
	}
	return err
}

func (s *grpcClientStream) Context() context.Context {
	return s.s.Context()
}

func (s *grpcClientStream) SendMsg(m interface{}) (err error) {
	s.c.incUsages()
	defer s.c.decUsages()
	err = s.s.SendMsg(m)
	if err != nil && s.wrapping {
		err = errors.Error(errors.MapGRPCError(err))
	}
	return
}

func (s *grpcClientStream) RecvMsg(m interface{}) (err error) {
	s.c.incUsages()
	defer s.c.decUsages()

	defer func() {
		onDone := s.recv(errors.HideEOF(err))
		if err != nil {
			onDone(s.c.GetState(), errors.HideEOF(err))
			s.onDone(s.s.Context())
		}
	}()

	err = s.s.RecvMsg(m)

	if err != nil {
		if s.wrapping {
			return errors.Error(errors.MapGRPCError(err))
		}
		return err
	}

	if s.wrapping {
		if operation, ok := m.(wrap.StreamOperationResponse); ok {
			if s := operation.GetStatus(); s != Ydb.StatusIds_SUCCESS {
				err = errors.NewOpError(errors.WithOEOperation(operation))
			}
		}
	}

	return err
}
