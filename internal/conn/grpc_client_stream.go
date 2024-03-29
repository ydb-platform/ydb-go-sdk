package conn

import (
	"context"
	"io"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/wrap"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type grpcClientStream struct {
	grpc.ClientStream
	ctx      context.Context
	c        *lazyConn
	wrapping bool
	traceID  string
	sentMark *modificationMark
	onDone   func(ctx context.Context, md metadata.MD)
}

func (s *grpcClientStream) CloseSend() (finalErr error) {
	onDone := trace.DriverOnConnStreamCloseSend(s.c.config.Trace(), &s.ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/conn.(*grpcClientStream).CloseSend"),
	)
	defer func() {
		onDone(finalErr)
	}()

	locked, unlock := s.c.inUse.TryLock()
	if !locked {
		return xerrors.WithStackTrace(errClosedConnection)
	}
	defer unlock()

	stop := s.c.lastUsage.Start()
	defer stop()

	err := s.ClientStream.CloseSend()
	if err != nil {
		if !s.wrapping {
			return err
		}

		if !xerrors.IsTransportError(err) {
			return xerrors.WithStackTrace(err)
		}

		if s.sentMark.canRetry() {
			return s.wrapError(
				xerrors.Retryable(
					xerrors.Transport(err,
						xerrors.WithAddress(s.c.Address()),
						xerrors.WithTraceID(s.traceID),
					),
					xerrors.WithName("CloseSend"),
				),
			)
		}

		return s.wrapError(xerrors.Transport(err,
			xerrors.WithAddress(s.c.Address()),
			xerrors.WithTraceID(s.traceID),
		))
	}

	return nil
}

func (s *grpcClientStream) SendMsg(m interface{}) (finalErr error) {
	onDone := trace.DriverOnConnStreamSendMsg(s.c.config.Trace(), &s.ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/conn.(*grpcClientStream).SendMsg"),
	)
	defer func() {
		onDone(finalErr)
	}()

	locked, unlock := s.c.inUse.TryLock()
	if !locked {
		return xerrors.WithStackTrace(errClosedConnection)
	}
	defer unlock()

	stop := s.c.lastUsage.Start()
	defer stop()

	err := s.ClientStream.SendMsg(m)
	if err != nil {
		if !s.wrapping {
			return err
		}

		if !xerrors.IsTransportError(err) {
			return xerrors.WithStackTrace(err)
		}

		if s.sentMark.canRetry() {
			return s.wrapError(
				xerrors.Retryable(
					xerrors.Transport(err,
						xerrors.WithAddress(s.c.Address()),
						xerrors.WithTraceID(s.traceID),
					),
					xerrors.WithName("SendMsg"),
				),
			)
		}

		return s.wrapError(xerrors.Transport(err,
			xerrors.WithAddress(s.c.Address()),
			xerrors.WithTraceID(s.traceID),
		))
	}

	return nil
}

func (s *grpcClientStream) RecvMsg(m interface{}) (finalErr error) {
	onDone := trace.DriverOnConnStreamRecvMsg(s.c.config.Trace(), &s.ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/conn.(*grpcClientStream).RecvMsg"),
	)
	defer func() {
		onDone(finalErr)
	}()

	locked, unlock := s.c.inUse.TryLock()
	if !locked {
		return xerrors.WithStackTrace(errClosedConnection)
	}
	defer unlock()

	stop := s.c.lastUsage.Start()
	defer stop()

	defer func() {
		if finalErr != nil {
			md := s.ClientStream.Trailer()
			s.onDone(s.ctx, md)
		}
	}()

	err := s.ClientStream.RecvMsg(m)
	if err != nil {
		if xerrors.Is(err, io.EOF) || !s.wrapping {
			return io.EOF
		}

		if !xerrors.IsTransportError(err) {
			return xerrors.WithStackTrace(err)
		}

		if s.sentMark.canRetry() {
			return s.wrapError(
				xerrors.Retryable(
					xerrors.Transport(err,
						xerrors.WithAddress(s.c.Address()),
						xerrors.WithTraceID(s.traceID),
					),
					xerrors.WithName("RecvMsg"),
				),
			)
		}

		return s.wrapError(xerrors.Transport(err,
			xerrors.WithAddress(s.c.Address()),
			xerrors.WithTraceID(s.traceID),
		))
	}

	if s.wrapping {
		if operation, ok := m.(wrap.StreamOperationResponse); ok {
			if status := operation.GetStatus(); status != Ydb.StatusIds_SUCCESS {
				return s.wrapError(
					xerrors.Operation(
						xerrors.FromOperation(operation),
						xerrors.WithAddress(s.c.Address()),
						xerrors.WithTraceID(s.traceID),
					),
				)
			}
		}
	}

	return nil
}

func (s *grpcClientStream) wrapError(err error) error {
	if err == nil {
		return nil
	}

	return xerrors.WithStackTrace(
		newConnError(s.c.endpoint.NodeID(), s.c.endpoint.Address(), err),
		xerrors.WithSkipDepth(1),
	)
}
