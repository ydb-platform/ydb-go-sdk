package conn

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/wrap"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type grpcClientStream struct {
	grpc.ClientStream
	c        *conn
	wrapping bool
	onDone   func(ctx context.Context)
	recv     func(error) func(trace.ConnState, error)
}

func (s *grpcClientStream) CloseSend() (err error) {
	err = s.ClientStream.CloseSend()

	if err != nil {
		if s.wrapping {
			return errors.WithStackTrace(
				errors.FromGRPCError(
					err,
					errors.WithAddress(s.c.Address()),
				),
			)
		}
		return errors.WithStackTrace(err)
	}

	return nil
}

func (s *grpcClientStream) SendMsg(m interface{}) (err error) {
	s.c.changeStreamUsages(1)
	defer s.c.changeStreamUsages(-1)

	err = s.ClientStream.SendMsg(m)

	if err != nil {
		if s.wrapping {
			return errors.WithStackTrace(
				errors.FromGRPCError(
					err,
					errors.WithAddress(s.c.Address()),
				),
			)
		}
		return errors.WithStackTrace(err)
	}

	return nil
}

func (s *grpcClientStream) RecvMsg(m interface{}) (err error) {
	s.c.changeStreamUsages(1)
	defer s.c.changeStreamUsages(-1)

	defer func() {
		onDone := s.recv(errors.HideEOF(err))
		if err != nil {
			onDone(s.c.GetState(), errors.HideEOF(err))
			s.onDone(s.ClientStream.Context())
		}
	}()

	err = s.ClientStream.RecvMsg(m)

	if err != nil {
		if s.wrapping {
			return errors.WithStackTrace(
				errors.FromGRPCError(
					err,
					errors.WithAddress(s.c.Address()),
				),
			)
		}
		return errors.WithStackTrace(err)
	}

	if s.wrapping {
		if operation, ok := m.(wrap.StreamOperationResponse); ok {
			if s := operation.GetStatus(); s != Ydb.StatusIds_SUCCESS {
				return errors.WithStackTrace(
					errors.NewOpError(
						errors.FromOperation(
							operation,
						),
					),
				)
			}
		}
	}

	return nil
}
