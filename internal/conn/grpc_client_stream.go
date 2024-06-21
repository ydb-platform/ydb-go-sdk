package conn

import (
	"context"
	"io"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type grpcClientStream struct {
	parentConn   *conn
	stream       grpc.ClientStream
	streamCtx    context.Context //nolint:containedctx
	streamCancel context.CancelFunc
	wrapping     bool
	traceID      string
	sentMark     *modificationMark
}

func (s *grpcClientStream) Header() (metadata.MD, error) {
	return s.stream.Header()
}

func (s *grpcClientStream) Trailer() metadata.MD {
	return s.stream.Trailer()
}

func (s *grpcClientStream) Context() context.Context {
	return s.stream.Context()
}

func (s *grpcClientStream) CloseSend() (err error) {
	var (
		ctx    = s.streamCtx
		onDone = trace.DriverOnConnStreamCloseSend(s.parentConn.config.Trace(), &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/conn.(*grpcClientStream).CloseSend"),
		)
	)
	defer func() {
		onDone(err)
	}()

	stop := s.parentConn.lastUsage.Start()
	defer stop()

	err = s.stream.CloseSend()

	if err != nil {
		if xerrors.IsContextError(err) {
			return xerrors.WithStackTrace(err)
		}

		if s.wrapping {
			return s.wrapError(
				xerrors.Transport(
					err,
					xerrors.WithAddress(s.parentConn.Address()),
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
		ctx    = s.streamCtx
		onDone = trace.DriverOnConnStreamSendMsg(s.parentConn.config.Trace(), &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/conn.(*grpcClientStream).SendMsg"),
		)
	)
	defer func() {
		onDone(err)
	}()

	stop := s.parentConn.lastUsage.Start()
	defer stop()

	err = s.stream.SendMsg(m)

	if err != nil {
		if xerrors.IsContextError(err) {
			return xerrors.WithStackTrace(err)
		}

		defer func() {
			s.parentConn.onTransportError(ctx, err)
		}()

		if s.wrapping {
			err = xerrors.Transport(err,
				xerrors.WithAddress(s.parentConn.Address()),
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

func (s *grpcClientStream) finish(err error) {
	s.streamCancel()
	trace.DriverOnConnStreamFinish(s.parentConn.config.Trace(), s.streamCtx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/conn.(*grpcClientStream).finish"), err,
	)
}

func (s *grpcClientStream) RecvMsg(m interface{}) (err error) {
	var (
		ctx    = s.streamCtx
		onDone = trace.DriverOnConnStreamRecvMsg(s.parentConn.config.Trace(), &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/conn.(*grpcClientStream).RecvMsg"),
		)
	)
	defer func() {
		onDone(err)
		if err != nil {
			meta.CallTrailerCallback(s.streamCtx, s.stream.Trailer())
		}
	}()

	stop := s.parentConn.lastUsage.Start()
	defer stop()

	err = s.stream.RecvMsg(m)

	if err != nil { //nolint:nestif
		if xerrors.IsContextError(err) {
			return xerrors.WithStackTrace(err)
		}

		defer func() {
			if !xerrors.Is(err, io.EOF) {
				s.parentConn.onTransportError(ctx, err)
			}
		}()

		if s.wrapping {
			err = xerrors.Transport(err,
				xerrors.WithAddress(s.parentConn.Address()),
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
		if operation, ok := m.(operation.Status); ok {
			if status := operation.GetStatus(); status != Ydb.StatusIds_SUCCESS {
				return s.wrapError(
					xerrors.Operation(
						xerrors.FromOperation(operation),
						xerrors.WithAddress(s.parentConn.Address()),
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
		newConnError(s.parentConn.endpoint.NodeID(), s.parentConn.endpoint.Address(), err),
		xerrors.WithSkipDepth(1),
	)
}
