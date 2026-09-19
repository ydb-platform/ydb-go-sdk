package conn

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/metadata"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestGrpcClientStream_Header(t *testing.T) {
	t.Run("ReturnsHeaderFromUnderlyingStream", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := mock.NewMockClientStream(ctrl)

		expectedMD := metadata.MD{"key": []string{"value"}}
		mockStream.EXPECT().Header().Return(expectedMD, nil)

		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
		}

		md, err := s.Header()
		require.NoError(t, err)
		require.Equal(t, expectedMD, md)
	})

	t.Run("ReturnsErrorFromUnderlyingStream", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := mock.NewMockClientStream(ctrl)

		expectedErr := fmt.Errorf("header error")
		mockStream.EXPECT().Header().Return(metadata.MD{}, expectedErr)

		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
		}

		md, err := s.Header()
		require.Error(t, err)
		require.Empty(t, md)
	})
}

func TestGrpcClientStream_Trailer(t *testing.T) {
	t.Run("ReturnsTrailerFromUnderlyingStream", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := mock.NewMockClientStream(ctrl)

		expectedMD := metadata.MD{"trailer-key": []string{"trailer-value"}}
		mockStream.EXPECT().Trailer().Return(expectedMD)

		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
		}

		md := s.Trailer()
		require.Equal(t, expectedMD, md)
	})
}

func TestGrpcClientStream_Context(t *testing.T) {
	t.Run("ReturnsContextFromUnderlyingStream", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := mock.NewMockClientStream(ctrl)

		expectedCtx := context.WithValue(t.Context(), "key", "value") //nolint:revive,staticcheck
		mockStream.EXPECT().Context().Return(expectedCtx)

		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
		}

		ctx := s.Context()
		require.Equal(t, expectedCtx, ctx)
	})
}

func TestGrpcClientStream_CloseSend(t *testing.T) {
	t.Run("SuccessfulClose", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := mock.NewMockClientStream(ctrl)

		mockStream.EXPECT().CloseSend().Return(nil)

		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			requestCtx: t.Context(),
			wrapping:   true,
			traceID:    "test-trace-id",
		}

		err := s.CloseSend()
		require.NoError(t, err)
	})

	t.Run("ContextError", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := mock.NewMockClientStream(ctrl)

		mockStream.EXPECT().CloseSend().Return(context.Canceled)

		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			requestCtx: t.Context(),
			wrapping:   true,
			traceID:    "test-trace-id",
		}

		err := s.CloseSend()
		require.Error(t, err)
		require.True(t, xerrors.IsContextError(err))
	})

	t.Run("StreamContextDoneReturnsNonTransportError", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := mock.NewMockClientStream(ctrl)

		// Use a non-gRPC-status error that gRPC may return on stream termination.
		// IsContextError returns false for such errors, so the old code fell through
		// to transport wrapping even when the stream context was already cancelled.
		streamErr := errors.New("stream transport: connection closed")
		mockStream.EXPECT().CloseSend().Return(streamErr)

		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		cancelledCtx, cancel := context.WithCancel(t.Context())
		cancel() // cancel the stream context before calling CloseSend

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			requestCtx: cancelledCtx,
			wrapping:   true,
			traceID:    "test-trace-id",
		}

		err := s.CloseSend()
		require.Error(t, err)
		// When the stream context is done, the error must NOT be wrapped as a
		// transport error regardless of what gRPC returned.
		require.True(t, xerrors.IsTransportError(err))
		require.ErrorIs(t, err, streamErr)
	})

	t.Run("TransportErrorWithWrapping", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := mock.NewMockClientStream(ctrl)

		mockStream.EXPECT().CloseSend().Return(fmt.Errorf("transport error"))

		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		e := endpoint.New("test-endpoint:2135", endpoint.WithID(123))
		parentConn := newConn(e, config)

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			requestCtx: t.Context(),
			wrapping:   true,
			traceID:    "test-trace-id",
		}

		err := s.CloseSend()
		require.Error(t, err)
		require.True(t, xerrors.IsTransportError(err))
	})

	t.Run("ErrorWithoutWrapping", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := mock.NewMockClientStream(ctrl)

		expectedErr := fmt.Errorf("raw error")
		mockStream.EXPECT().CloseSend().Return(expectedErr)

		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			requestCtx: t.Context(),
			wrapping:   false,
		}

		err := s.CloseSend()
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
	})
}

func TestGrpcClientStream_SendMsg(t *testing.T) {
	t.Run("SuccessfulSend", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := mock.NewMockClientStream(ctrl)

		msg := &Ydb_Query.ExecuteQueryRequest{}
		mockStream.EXPECT().SendMsg(msg).Return(nil)

		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			requestCtx: t.Context(),
			wrapping:   true,
			sentMark:   &modificationMark{},
		}

		err := s.SendMsg(msg)
		require.NoError(t, err)
	})

	t.Run("ContextError", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := mock.NewMockClientStream(ctrl)

		msg := &Ydb_Query.ExecuteQueryRequest{}
		mockStream.EXPECT().SendMsg(msg).Return(context.DeadlineExceeded)

		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			requestCtx: t.Context(),
			wrapping:   true,
			sentMark:   &modificationMark{},
		}

		err := s.SendMsg(msg)
		require.Error(t, err)
		require.True(t, xerrors.IsContextError(err))
	})

	t.Run("StreamContextDoneReturnsNonTransportError", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := mock.NewMockClientStream(ctrl)

		msg := &Ydb_Query.ExecuteQueryRequest{}
		// Use a non-gRPC-status error that gRPC may return on stream termination.
		// IsContextError returns false for such errors, so the old code fell through
		// to transport wrapping even when the stream context was already cancelled.
		streamErr := errors.New("stream transport: connection closed")
		mockStream.EXPECT().SendMsg(msg).Return(streamErr)

		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		cancelledCtx, cancel := context.WithCancel(t.Context())
		cancel() // cancel the stream context before calling SendMsg

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			requestCtx: cancelledCtx,
			wrapping:   true,
			traceID:    "test-trace-id",
			sentMark:   &modificationMark{},
		}

		err := s.SendMsg(msg)
		require.Error(t, err)
		// When the stream context is done, the error must NOT be wrapped as a
		// transport error regardless of what gRPC returned.
		require.True(t, xerrors.IsTransportError(err))
		require.ErrorIs(t, err, streamErr)
	})

	t.Run("TransportErrorRetryable", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := mock.NewMockClientStream(ctrl)

		msg := &Ydb_Query.ExecuteQueryRequest{}
		mockStream.EXPECT().SendMsg(msg).Return(grpcStatus.Error(grpcCodes.Unavailable, "unavailable"))

		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			requestCtx: t.Context(),
			wrapping:   true,
			traceID:    "test-trace-id",
			sentMark:   &modificationMark{},
		}

		err := s.SendMsg(msg)
		require.Error(t, err)
		require.True(t, xerrors.IsTransportError(err))
		require.True(t, xerrors.IsRetryableError(err))
	})

	t.Run("TransportErrorNonRetryable", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := mock.NewMockClientStream(ctrl)

		msg := &Ydb_Query.ExecuteQueryRequest{}
		mockStream.EXPECT().SendMsg(msg).Return(grpcStatus.Error(grpcCodes.Unavailable, "unavailable"))

		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		e := endpoint.New("test-endpoint:2135", endpoint.WithID(123))
		parentConn := newConn(e, config)

		mark := &modificationMark{}
		mark.markDirty()

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			requestCtx: t.Context(),
			wrapping:   true,
			traceID:    "test-trace-id",
			sentMark:   mark,
		}

		err := s.SendMsg(msg)
		require.Error(t, err)
		require.True(t, xerrors.IsTransportError(err))
		require.False(t, xerrors.IsRetryableError(err))
	})

	t.Run("ErrorWithoutWrapping", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := mock.NewMockClientStream(ctrl)

		msg := &Ydb_Query.ExecuteQueryRequest{}
		expectedErr := fmt.Errorf("raw error")
		mockStream.EXPECT().SendMsg(msg).Return(expectedErr)

		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			requestCtx: t.Context(),
			wrapping:   false,
			sentMark:   &modificationMark{},
		}

		err := s.SendMsg(msg)
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
	})
}

func TestGrpcClientStream_RecvMsg(t *testing.T) {
	t.Run("SuccessfulReceive", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := mock.NewMockClientStream(ctrl)

		msg := &Ydb_Query.ExecuteQueryResponsePart{}
		mockStream.EXPECT().RecvMsg(msg).DoAndReturn(func(m any) error {
			resp := m.(*Ydb_Query.ExecuteQueryResponsePart)
			resp.Status = Ydb.StatusIds_SUCCESS

			return nil
		})

		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			requestCtx: t.Context(),
			wrapping:   true,
			sentMark:   &modificationMark{},
		}

		err := s.RecvMsg(msg)
		require.NoError(t, err)
	})

	t.Run("EOFError", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := mock.NewMockClientStream(ctrl)

		msg := &Ydb_Query.ExecuteQueryResponsePart{}
		mockStream.EXPECT().RecvMsg(msg).Return(io.EOF)

		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			requestCtx: t.Context(),
			wrapping:   true,
			sentMark:   &modificationMark{},
		}

		err := s.RecvMsg(msg)
		require.Equal(t, io.EOF, err)
	})

	t.Run("ContextError", func(t *testing.T) {
		t.Run("context.Canceled from RecvMsg", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockStream := mock.NewMockClientStream(ctrl)

			msg := &Ydb_Query.ExecuteQueryResponsePart{}
			mockStream.EXPECT().RecvMsg(msg).Return(context.Canceled)

			config := &mockConfig{
				dialTimeout: 5 * time.Second,
			}
			e := endpoint.New("test-endpoint:2135")
			parentConn := newConn(e, config)

			s := &grpcClientStream{
				parentConn: parentConn,
				stream:     mockStream,
				requestCtx: t.Context(),
				wrapping:   true,
				sentMark:   &modificationMark{},
			}

			err := s.RecvMsg(msg)
			require.Error(t, err)
			require.True(t, xerrors.IsContextError(err))
		})
		t.Run("stream context canceled", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockStream := mock.NewMockClientStream(ctrl)

			msg := &Ydb_Query.ExecuteQueryResponsePart{}
			mockStream.EXPECT().RecvMsg(msg).Return(grpcStatus.Error(grpcCodes.Canceled, ""))

			config := &mockConfig{
				dialTimeout: 5 * time.Second,
			}
			e := endpoint.New("test-endpoint:2135")
			parentConn := newConn(e, config)

			ctx, cancel := context.WithCancel(t.Context())
			cancel()

			s := &grpcClientStream{
				parentConn: parentConn,
				stream:     mockStream,
				requestCtx: ctx,
				wrapping:   true,
				sentMark:   &modificationMark{},
			}

			err := s.RecvMsg(msg)
			require.Error(t, err)
			require.True(t, xerrors.IsContextError(err))
			require.True(t, xerrors.IsTransportError(err))
		})
	})

	t.Run("StreamContextDoneReturnsNonTransportError", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := mock.NewMockClientStream(ctrl)

		msg := &Ydb_Query.ExecuteQueryResponsePart{}
		// Use a non-gRPC-status error that gRPC may return on stream termination.
		// IsContextError returns false for such errors, so the old code fell through
		// to transport wrapping even when the stream context was already cancelled.
		streamErr := errors.New("stream transport: connection closed")
		mockStream.EXPECT().RecvMsg(msg).Return(streamErr)

		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		cancelledCtx, cancel := context.WithCancel(t.Context())
		cancel() // cancel the stream context before calling RecvMsg

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			requestCtx: cancelledCtx,
			wrapping:   true,
			traceID:    "test-trace-id",
			sentMark:   &modificationMark{},
		}

		err := s.RecvMsg(msg)
		require.Error(t, err)
		// When the stream context is done, the error must NOT be wrapped as a
		// transport error regardless of what gRPC returned.
		require.True(t, xerrors.IsTransportError(err))
		require.True(t, xerrors.IsContextError(err))
		require.ErrorIs(t, err, streamErr)
	})

	t.Run("TransportErrorRetryable", func(t *testing.T) {
		t.Run("Unavailable", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockStream := mock.NewMockClientStream(ctrl)

			msg := &Ydb_Query.ExecuteQueryResponsePart{}
			mockStream.EXPECT().RecvMsg(msg).Return(grpcStatus.Error(grpcCodes.Unavailable, "unavailable"))

			config := &mockConfig{
				dialTimeout: 5 * time.Second,
			}
			e := endpoint.New("test-endpoint:2135")
			parentConn := newConn(e, config)

			s := &grpcClientStream{
				parentConn: parentConn,
				stream:     mockStream,
				requestCtx: t.Context(),
				wrapping:   true,
				traceID:    "test-trace-id",
				sentMark:   &modificationMark{},
			}

			err := s.RecvMsg(msg)
			require.Error(t, err)
			require.True(t, xerrors.IsTransportError(err))
			require.True(t, xerrors.IsRetryableError(err))
		})
		t.Run("Cancelled", func(t *testing.T) {
			t.Run("sentMark.canRetry()==false", func(t *testing.T) {
				ctrl := gomock.NewController(t)
				mockStream := mock.NewMockClientStream(ctrl)

				msg := &Ydb_Query.ExecuteQueryResponsePart{}
				mockStream.EXPECT().RecvMsg(msg).Return(grpcStatus.Error(grpcCodes.Canceled, "Cancelled on the server side"))

				ctx, cancel := context.WithCancel(t.Context())
				cancel()

				// Cleaner existing pattern:
				mark := &modificationMark{}
				mark.markDirty()

				s := &grpcClientStream{
					parentConn: &conn{
						config:   &mockConfig{},
						endpoint: endpoint.New("test-endpoint:2135"),
					},
					stream:     mockStream,
					requestCtx: ctx,
					wrapping:   true,
					sentMark:   mark,
				}

				err := s.RecvMsg(msg)
				require.Error(t, err)

				check := retry.Check(err)
				require.EqualValues(t, grpcCodes.Canceled, check.StatusCode())
				require.EqualValues(t, backoff.TypeFast, check.BackoffType())
				require.True(t, check.MustRetry(true))
				require.False(t, check.MustRetry(false))
			})
			t.Run("sentMark.canRetry()==true", func(t *testing.T) {
				ctrl := gomock.NewController(t)
				mockStream := mock.NewMockClientStream(ctrl)

				msg := &Ydb_Query.ExecuteQueryResponsePart{}
				mockStream.EXPECT().RecvMsg(msg).Return(grpcStatus.Error(grpcCodes.Canceled, context.Canceled.Error()))

				ctx, cancel := context.WithCancel(t.Context())
				cancel()

				s := &grpcClientStream{
					parentConn: &conn{
						config:   &mockConfig{},
						endpoint: endpoint.New("test-endpoint:2135"),
					},
					stream:     mockStream,
					requestCtx: ctx,
					wrapping:   true,
					sentMark:   &modificationMark{},
				}

				err := s.RecvMsg(msg)
				require.Error(t, err)

				check := retry.Check(err)
				require.EqualValues(t, grpcCodes.Canceled, check.StatusCode())
				require.EqualValues(t, backoff.TypeFast, check.BackoffType())
				require.True(t, check.MustRetry(true))
				require.True(t, check.MustRetry(false))
			})
		})
	})

	t.Run("TransportErrorNonRetryable", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := mock.NewMockClientStream(ctrl)

		msg := &Ydb_Query.ExecuteQueryResponsePart{}
		mockStream.EXPECT().RecvMsg(msg).Return(grpcStatus.Error(grpcCodes.Unavailable, "unavailable"))

		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		e := endpoint.New("test-endpoint:2135", endpoint.WithID(123))
		parentConn := newConn(e, config)

		mark := &modificationMark{}
		mark.markDirty()

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			requestCtx: t.Context(),
			wrapping:   true,
			traceID:    "test-trace-id",
			sentMark:   mark,
		}

		err := s.RecvMsg(msg)
		require.Error(t, err)
		require.True(t, xerrors.IsTransportError(err))
		require.False(t, xerrors.IsRetryableError(err))
	})

	t.Run("ErrorWithoutWrapping", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := mock.NewMockClientStream(ctrl)

		msg := &Ydb_Query.ExecuteQueryResponsePart{}
		expectedErr := fmt.Errorf("raw error")
		mockStream.EXPECT().RecvMsg(msg).Return(expectedErr)

		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			requestCtx: t.Context(),
			wrapping:   false,
			sentMark:   &modificationMark{},
		}

		err := s.RecvMsg(msg)
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
	})

	t.Run("OperationError", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := mock.NewMockClientStream(ctrl)

		msg := &Ydb_Query.ExecuteQueryResponsePart{}
		mockStream.EXPECT().RecvMsg(msg).DoAndReturn(func(m any) error {
			resp := m.(*Ydb_Query.ExecuteQueryResponsePart)
			resp.Status = Ydb.StatusIds_UNAVAILABLE

			return nil
		})
		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		e := endpoint.New("test-endpoint:2135", endpoint.WithID(123))
		parentConn := newConn(e, config)
		canceled := false

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			requestCtx: t.Context(),
			grpcCancel: func() {
				canceled = true
			},
			wrapping: true,
			sentMark: &modificationMark{},
		}

		err := s.RecvMsg(msg)
		require.Error(t, err)
		require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_UNAVAILABLE))
		require.True(t, canceled)
	})

	t.Run("OperationErrorWithoutWrapping", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := mock.NewMockClientStream(ctrl)

		msg := &Ydb_Query.ExecuteQueryResponsePart{}
		mockStream.EXPECT().RecvMsg(msg).DoAndReturn(func(m any) error {
			resp := m.(*Ydb_Query.ExecuteQueryResponsePart)
			resp.Status = Ydb.StatusIds_UNAVAILABLE

			return nil
		})

		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			requestCtx: t.Context(),
			wrapping:   false,
			sentMark:   &modificationMark{},
		}

		err := s.RecvMsg(msg)
		require.NoError(t, err)
	})

	t.Run("OperationErrorDoesNotRaceOnTrailer", func(t *testing.T) {
		// Regression test: on a non-success YDB operation status the underlying
		// gRPC RecvMsg returns nil, so the stream is not finished. Before the fix,
		// RecvMsg read Trailer() while the simulated transport was mutating it.
		xtest.TestManyTimes(t, func(t testing.TB) {
			fake := &fakeTrailerStream{
				recv: func(m any) error {
					resp := m.(*Ydb_Query.ExecuteQueryResponsePart)
					resp.Status = Ydb.StatusIds_UNAVAILABLE

					return nil // underlying gRPC RecvMsg succeeds
				},
				trailerReadStarted: make(chan struct{}),
				writerSelected:     make(chan struct{}),
				transportStop:      make(chan struct{}),
				transportDone:      make(chan struct{}),
			}
			fake.startTransport()
			defer fake.stopTransport()

			config := &mockConfig{
				dialTimeout: 5 * time.Second,
			}
			e := endpoint.New("test-endpoint:2135", endpoint.WithID(123))
			parentConn := newConn(e, config)

			s := &grpcClientStream{
				parentConn: parentConn,
				stream:     fake,
				requestCtx: t.Context(),
				grpcCancel: func() {},
				wrapping:   true,
				sentMark:   &modificationMark{},
			}

			msg := &Ydb_Query.ExecuteQueryResponsePart{}
			err := s.RecvMsg(msg)

			require.Error(t, err)
			require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_UNAVAILABLE))
			require.Zero(t, fake.trailerCalls.Load(),
				"Trailer() must not be read before the stream finishes")
		})
	})
}

func TestGrpcClientStream_Finish(t *testing.T) {
	t.Run("CallsCancelOnFinish", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := mock.NewMockClientStream(ctrl)

		config := &mockConfig{
			dialTimeout: 5 * time.Second,
			driverTrace: &trace.Driver{},
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		ctx, cancel := context.WithCancel(t.Context())
		called := false
		wrappedCancel := func() {
			called = true
			cancel()
		}

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			requestCtx: ctx,
			grpcCancel: wrappedCancel,
		}

		s.finish(nil)
		require.True(t, called)
	})

	t.Run("FinishWithError", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := mock.NewMockClientStream(ctrl)

		config := &mockConfig{
			dialTimeout: 5 * time.Second,
			driverTrace: &trace.Driver{},
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		ctx, cancel := context.WithCancel(t.Context())

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			requestCtx: ctx,
			grpcCancel: cancel,
		}

		testErr := fmt.Errorf("test error")
		s.finish(testErr)
		// Should not panic
	})

	t.Run("CallsTrailerCallback", func(t *testing.T) {
		config := &mockConfig{
			dialTimeout: 5 * time.Second,
			driverTrace: &trace.Driver{},
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		trailer := metadata.Pairs("x-ydb-server-hints", "session-close")
		var callbackTrailer metadata.MD
		ctx, cancel := context.WithCancel(meta.WithTrailerCallback(t.Context(), func(md metadata.MD) {
			callbackTrailer = md
		}))

		s := &grpcClientStream{
			parentConn: parentConn,
			trailer:    trailer,
			requestCtx: ctx,
			grpcCancel: cancel,
		}

		s.finish(nil)

		require.Equal(t, trailer, callbackTrailer)
		require.ErrorIs(t, ctx.Err(), context.Canceled)
	})
}

func TestConn_NewStreamCallsTrailerCallbackOnRecvEnd(t *testing.T) {
	tests := []struct {
		name    string
		recvErr error
	}{
		{
			name:    "EOF",
			recvErr: io.EOF,
		},
		{
			name:    "TransportError",
			recvErr: grpcStatus.Error(grpcCodes.Unavailable, "unavailable"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			trailer := metadata.Pairs("x-ydb-server-hints", "session-close")
			var callerTrailer metadata.MD
			rawStream := &finishCaptureStream{
				recv: func(any) error {
					return test.recvErr
				},
				trailer:    trailer,
				finishDone: make(chan struct{}),
			}
			rawConn := &finishCaptureConn{
				stream: rawStream,
			}

			config := &mockConfig{
				dialTimeout: 5 * time.Second,
				driverTrace: &trace.Driver{},
			}
			e := endpoint.New("test-endpoint:2135")
			parentConn := newConn(e, config)
			parentConn.grpcConn = rawConn

			var callbackTrailer metadata.MD
			ctx := meta.WithTrailerCallback(t.Context(), func(md metadata.MD) {
				callbackTrailer = md
			})

			stream, err := parentConn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true},
				"/test.Service/Stream", grpc.Trailer(&callerTrailer))
			require.NoError(t, err)
			require.Len(t, rawStream.trailerAddrs, 2)
			require.NotNil(t, rawStream.onFinish)

			err = stream.RecvMsg(&Ydb_Query.ExecuteQueryResponsePart{})
			require.Error(t, err)
			if errors.Is(test.recvErr, io.EOF) {
				require.ErrorIs(t, err, io.EOF)
			} else {
				require.True(t, xerrors.IsTransportError(err))
			}
			require.Equal(t, trailer, callbackTrailer)
			require.Equal(t, trailer, callerTrailer)
		})
	}
}

func TestConn_NewStreamCancelsOnOperationError(t *testing.T) {
	trailer := metadata.Pairs("x-ydb-server-hints", "session-close")
	rawStream := &finishCaptureStream{
		recv: func(m any) error {
			response := m.(*Ydb_Query.ExecuteQueryResponsePart)
			response.Status = Ydb.StatusIds_UNAVAILABLE

			return nil
		},
		trailer:    trailer,
		finishDone: make(chan struct{}),
	}
	rawConn := &finishCaptureConn{
		stream: rawStream,
	}

	config := &mockConfig{
		dialTimeout: 5 * time.Second,
		driverTrace: &trace.Driver{},
	}
	e := endpoint.New("test-endpoint:2135")
	parentConn := newConn(e, config)
	parentConn.grpcConn = rawConn

	var callbackTrailer metadata.MD
	ctx := meta.WithTrailerCallback(t.Context(), func(md metadata.MD) {
		callbackTrailer = md
	})

	stream, err := parentConn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, "/test.Service/Stream")
	require.NoError(t, err)

	err = stream.RecvMsg(&Ydb_Query.ExecuteQueryResponsePart{})
	require.Error(t, err)
	require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_UNAVAILABLE))
	require.Eventually(t, func() bool {
		select {
		case <-rawStream.finishDone:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
	require.Nil(t, callbackTrailer)
}

// fakeTrailerStream is a grpc.ClientStream whose transport goroutine mutates
// trailer metadata after Trailer starts reading it. This makes the old
// RecvMsg trailer read fail deterministically under the race detector.
type fakeTrailerStream struct {
	grpc.ClientStream

	recv               func(m any) error
	trailer            metadata.MD
	trailerCalls       atomic.Int32
	trailerReadOnce    sync.Once
	trailerReadStarted chan struct{}
	writerSelected     chan struct{}
	transportStop      chan struct{}
	transportDone      chan struct{}
}

func (f *fakeTrailerStream) startTransport() {
	go func() {
		defer close(f.transportDone)
		select {
		case <-f.trailerReadStarted:
			close(f.writerSelected)
			f.trailer = metadata.MD{"x-ydb-server-hints": []string{"session-close"}}
		case <-f.transportStop:
		}
	}()
}

func (f *fakeTrailerStream) stopTransport() {
	close(f.transportStop)
	<-f.transportDone
}

func (f *fakeTrailerStream) RecvMsg(m any) error {
	return f.recv(m)
}

func (f *fakeTrailerStream) Trailer() metadata.MD {
	f.trailerCalls.Add(1)
	f.trailerReadOnce.Do(func() {
		close(f.trailerReadStarted)
	})
	<-f.writerSelected

	return f.trailer
}

type finishCaptureConn struct {
	stream *finishCaptureStream
}

func (f *finishCaptureConn) Invoke(context.Context, string, any, any, ...grpc.CallOption) error {
	return nil
}

func (f *finishCaptureConn) NewStream(
	ctx context.Context,
	_ *grpc.StreamDesc,
	_ string,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	for _, opt := range opts {
		switch opt := opt.(type) {
		case grpc.TrailerCallOption:
			f.stream.trailerAddrs = append(f.stream.trailerAddrs, opt.TrailerAddr)
		case grpc.OnFinishCallOption:
			f.stream.onFinish = opt.OnFinish
		}
	}
	go func() {
		<-ctx.Done()
		f.stream.finish(ctx.Err())
	}()

	return f.stream, nil
}

func (f *finishCaptureConn) Close() error {
	return nil
}

func (f *finishCaptureConn) GetState() connectivity.State {
	return connectivity.Ready
}

type finishCaptureStream struct {
	grpc.ClientStream

	recv         func(m any) error
	trailer      metadata.MD
	trailerAddrs []*metadata.MD
	onFinish     func(error)
	finishOnce   sync.Once
	finishDone   chan struct{}
}

func (f *finishCaptureStream) RecvMsg(m any) error {
	err := f.recv(m)
	if err != nil {
		f.finish(err)
	}

	return err
}

func (f *finishCaptureStream) finish(err error) {
	f.finishOnce.Do(func() {
		if !errors.Is(err, context.Canceled) {
			for _, trailerAddr := range f.trailerAddrs {
				*trailerAddr = f.trailer
			}
		}
		if errors.Is(err, io.EOF) {
			err = nil
		}
		f.onFinish(err)
		close(f.finishDone)
	})
}
