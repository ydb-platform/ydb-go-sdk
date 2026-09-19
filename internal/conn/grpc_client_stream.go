package conn

import (
	"context"
	"io"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type grpcClientStream struct {
	parentConn      *conn
	stream          grpc.ClientStream
	trailer         metadata.MD
	finishedTrailer atomic.Pointer[metadata.MD]
	requestCtx      context.Context //nolint:containedctx
	grpcCancel      context.CancelFunc
	wrapping        bool
	traceID         string
	sentMark        *modificationMark
}

func (s *grpcClientStream) Header() (metadata.MD, error) {
	return s.stream.Header()
}

// Trailer returns a snapshot of the server trailers captured after the
// underlying gRPC stream finishes. It returns nil while the stream is active.
func (s *grpcClientStream) Trailer() metadata.MD {
	if trailer := s.finishedTrailer.Load(); trailer != nil && *trailer != nil {
		return (*trailer).Copy()
	}

	return nil
}

func (s *grpcClientStream) Context() context.Context {
	return s.stream.Context()
}

// Endpoint returns the endpoint of the connection used by this stream.
// It implements the optional interface used by topic writer for session logging.
func (s *grpcClientStream) Endpoint() endpoint.Endpoint {
	return s.parentConn.Endpoint()
}

func (s *grpcClientStream) CloseSend() (err error) {
	stopUsage := s.parentConn.startUsage()
	defer stopUsage()

	var (
		ctx    = s.requestCtx
		onDone = gtrace.DriverOnConnStreamCloseSend(s.parentConn.config.Trace(), &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.(*grpcClientStream).CloseSend"),
		)
	)
	defer func() {
		onDone(err)
	}()

	err = s.stream.CloseSend()
	if err != nil {
		if !s.wrapping {
			return err
		}

		return xerrors.WithStackTrace(xerrors.Join(
			s.requestCtx.Err(),
			xerrors.Transport(err,
				xerrors.WithAddress(s.parentConn.Address()),
				xerrors.WithNodeID(s.parentConn.NodeID()),
				xerrors.WithTraceID(s.traceID),
			),
		))
	}

	return nil
}

func (s *grpcClientStream) SendMsg(m any) (err error) {
	stopUsage := s.parentConn.startUsage()
	defer stopUsage()

	var (
		ctx    = s.requestCtx
		onDone = gtrace.DriverOnConnStreamSendMsg(s.parentConn.config.Trace(), &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.(*grpcClientStream).SendMsg"),
		)
	)
	defer func() {
		onDone(err)
	}()

	err = s.stream.SendMsg(m)
	if err != nil {
		if !s.wrapping {
			return err
		}

		if s.sentMark.canRetry() {
			return xerrors.WithStackTrace(xerrors.Retryable(
				xerrors.Join(
					s.requestCtx.Err(),
					xerrors.Transport(err, xerrors.WithTraceID(s.traceID)),
				),
				xerrors.WithName("SendMsg"),
			))
		}

		return xerrors.WithStackTrace(xerrors.Join(
			s.requestCtx.Err(),
			xerrors.Transport(err,
				xerrors.WithAddress(s.parentConn.Address()),
				xerrors.WithNodeID(s.parentConn.NodeID()),
				xerrors.WithTraceID(s.traceID),
			),
		))
	}

	return nil
}

func (s *grpcClientStream) finish(err error) {
	// grpc-go v1.78.0 applies TrailerCallOption.after before invoking OnFinish
	// callbacks from clientStream.finish. Publish the now-immutable trailer;
	// real-gRPC tests cover this ordering when the dependency is upgraded.
	trailer := s.trailer
	if trailer != nil {
		trailer = trailer.Copy()
	}
	s.finishedTrailer.Store(&trailer)
	// Trailer callbacks run inline on gRPC's completion path and must not block.
	meta.CallTrailerCallback(s.requestCtx, s.Trailer())
	gtrace.DriverOnConnStreamFinish(s.parentConn.config.Trace(), s.requestCtx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.(*grpcClientStream).finish"), err,
	)
	s.grpcCancel()
}

func (s *grpcClientStream) RecvMsg(m any) (err error) {
	stopUsage := s.parentConn.startUsage()
	defer stopUsage()

	var (
		ctx    = s.requestCtx
		onDone = gtrace.DriverOnConnStreamRecvMsg(s.parentConn.config.Trace(), &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.(*grpcClientStream).RecvMsg"),
		)
	)
	defer func() {
		onDone(err)
	}()

	err = s.stream.RecvMsg(m)
	if err != nil {
		if xerrors.Is(err, io.EOF) {
			return io.EOF
		}

		if !s.wrapping {
			return err
		}

		if s.sentMark.canRetry() {
			return xerrors.WithStackTrace(xerrors.Retryable(
				xerrors.Join(
					s.requestCtx.Err(),
					xerrors.Transport(err, xerrors.WithTraceID(s.traceID)),
				),
				xerrors.WithName("RecvMsg"),
			))
		}

		return xerrors.WithStackTrace(xerrors.Join(
			s.requestCtx.Err(),
			xerrors.Transport(err,
				xerrors.WithAddress(s.parentConn.Address()),
				xerrors.WithNodeID(s.parentConn.NodeID()),
				xerrors.WithTraceID(s.traceID),
			),
		))
	}

	if s.wrapping {
		if operation, ok := m.(operation.Status); ok {
			if status := operation.GetStatus(); status != Ydb.StatusIds_SUCCESS {
				return xerrors.WithStackTrace(xerrors.Operation(
					xerrors.FromOperation(operation),
					xerrors.WithAddress(s.parentConn.Address()),
					xerrors.WithNodeID(s.parentConn.NodeID()),
					xerrors.WithTraceID(s.traceID),
				))
			}
		}
	}

	return nil
}
