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
	c        *conn
	wrapping bool
	traceID  string
	sentMark *modificationMark
	onDone   func(ctx context.Context, md metadata.MD)
}

func (s *grpcClientStream) CloseSend() (err error) {
	var (
		ctx    = s.Context()
		onDone = trace.DriverOnConnStreamCloseSend(s.c.config.Trace(), &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/conn.(*grpcClientStream).CloseSend"),
		)
	)
	defer func() {
		onDone(err)
	}()

	stop := s.c.lastUsage.Start()
	defer stop()

	err = s.ClientStream.CloseSend()

	if err != nil {
		if xerrors.IsContextError(err) {
			return xerrors.WithStackTrace(err)
		}

		if s.wrapping {
			return s.wrapError(
				xerrors.Transport(
					err,
					xerrors.WithAddress(s.c.Address()),
					xerrors.WithTraceID(s.traceID),
				),
			)
		}

		return s.wrapError(err)
	}

	return nil
}

func (s *grpcClientStream) SendMsg(m interface{}) (err error) {
	var (
		ctx    = s.Context()
		onDone = trace.DriverOnConnStreamSendMsg(s.c.config.Trace(), &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/conn.(*grpcClientStream).SendMsg"),
		)
	)
	defer func() {
		onDone(err)
	}()

	stop := s.c.lastUsage.Start()
	defer stop()

	err = s.ClientStream.SendMsg(m)

	if err != nil {
		if xerrors.IsContextError(err) {
			return xerrors.WithStackTrace(err)
		}

		defer func() {
			s.c.onTransportError(ctx, err)
		}()

		if s.wrapping {
			err = xerrors.Transport(err,
				xerrors.WithAddress(s.c.Address()),
				xerrors.WithTraceID(s.traceID),
			)
			if s.sentMark.canRetry() {
				return s.wrapError(xerrors.Retryable(err,
					xerrors.WithName("SendMsg"),
				))
			}

			return s.wrapError(err)
		}

		return err
	}

	return nil
}

func (s *grpcClientStream) RecvMsg(m interface{}) (err error) {
	var (
		ctx    = s.Context()
		onDone = trace.DriverOnConnStreamRecvMsg(s.c.config.Trace(), &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/conn.(*grpcClientStream).RecvMsg"),
		)
	)
	defer func() {
		onDone(err)
	}()

	stop := s.c.lastUsage.Start()
	defer stop()

	defer func() {
		if err != nil {
			md := s.ClientStream.Trailer()
			s.onDone(ctx, md)
		}
	}()

	err = s.ClientStream.RecvMsg(m)

	if err != nil { //nolint:nestif
		if xerrors.IsContextError(err) {
			return xerrors.WithStackTrace(err)
		}

		defer func() {
			if !xerrors.Is(err, io.EOF) {
				s.c.onTransportError(ctx, err)
			}
		}()

		if s.wrapping {
			err = xerrors.Transport(err,
				xerrors.WithAddress(s.c.Address()),
			)
			if s.sentMark.canRetry() {
				return s.wrapError(xerrors.Retryable(err,
					xerrors.WithName("RecvMsg"),
				))
			}

			return s.wrapError(err)
		}

		return err
	}

	if s.wrapping {
		if operation, ok := m.(wrap.StreamOperationResponse); ok {
			if status := operation.GetStatus(); status != Ydb.StatusIds_SUCCESS {
				return s.wrapError(
					xerrors.Operation(
						xerrors.FromOperation(operation),
						xerrors.WithAddress(s.c.Address()),
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
