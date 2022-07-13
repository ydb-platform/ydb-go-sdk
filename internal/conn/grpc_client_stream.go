package conn

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/wrap"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type grpcClientStream struct {
	grpc.ClientStream
	c        *conn
	wrapping bool
	sentMark *modificationMark
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
	cancel := createPinger(s.c)
	defer cancel(xerrors.WithStackTrace(errors.New("send msg finished")))

	err = s.ClientStream.SendMsg(m)

	if err != nil {
		if s.wrapping {
			err = xerrors.FromGRPCError(err,
				xerrors.WithAddress(s.c.Address()),
			)
			if s.sentMark.safeToRetry() {
				err = xerrors.Retryable(err,
					xerrors.WithName("SendMsg"),
					xerrors.WithDeleteSession(),
				)
			}
			err = xerrors.WithStackTrace(err)
		}

		s.c.onTransportError(s.Context(), err)

		return err
	}

	return nil
}

func (s *grpcClientStream) RecvMsg(m interface{}) (err error) {
	cancel := createPinger(s.c)
	defer cancel(xerrors.WithStackTrace(errors.New("receive msg finished")))

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
			err = xerrors.FromGRPCError(err,
				xerrors.WithAddress(s.c.Address()),
			)
			if s.sentMark.safeToRetry() {
				err = xerrors.Retryable(err,
					xerrors.WithName("RecvMsg"),
					xerrors.WithDeleteSession(),
				)
			}
			err = xerrors.WithStackTrace(err)
		}

		s.c.onTransportError(s.Context(), err)

		return err
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

func createPinger(c *conn) xcontext.CancelErrFunc {
	c.touchLastUsage()
	ctx, cancel := xcontext.WithErrCancel(context.Background())
	go func() {
		ticker := time.NewTicker(time.Second)
		ctxDone := ctx.Done()
		for {
			select {
			case <-ctxDone:
				ticker.Stop()
				return
			case <-ticker.C:
				c.touchLastUsage()
			}
		}
	}()

	return cancel
}
