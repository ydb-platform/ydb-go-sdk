package conn

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/wrap"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
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
			return xerrors.WithStackTrace(
				xerrors.FromGRPCError(
					err,
					xerrors.WithAddress(s.c.Address()),
				),
			)
		}
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (s *grpcClientStream) SendMsg(m interface{}) (err error) {
	s.c.changeStreamUsages(1)
	defer s.c.changeStreamUsages(-1)

	err = s.ClientStream.SendMsg(m)

	if err != nil {
		if s.wrapping {
			return xerrors.WithStackTrace(
				xerrors.FromGRPCError(
					err,
					xerrors.WithAddress(s.c.Address()),
				),
			)
		}
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (s *grpcClientStream) RecvMsg(m interface{}) (err error) {
	s.c.changeStreamUsages(1)
	defer s.c.changeStreamUsages(-1)

	defer func() {
		onDone := s.recv(xerrors.HideEOF(err))
		if err != nil {
			onDone(s.c.GetState(), xerrors.HideEOF(err))
			s.onDone(s.ClientStream.Context())
		}
	}()

	err = s.ClientStream.RecvMsg(m)

	if err != nil {
		if s.wrapping {
			return xerrors.WithStackTrace(
				xerrors.FromGRPCError(
					err,
					xerrors.WithAddress(s.c.Address()),
				),
			)
		}
		return xerrors.WithStackTrace(err)
	}

	if s.wrapping {
		if operation, ok := m.(wrap.StreamOperationResponse); ok {
			if s := operation.GetStatus(); s != Ydb.StatusIds_SUCCESS {
				return xerrors.WithStackTrace(
					xerrors.Operation(
						xerrors.FromOperation(
							operation,
						),
					),
				)
			}
		}
	}

	return nil
}
