package conn

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
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
	"google.golang.org/grpc/credentials/insecure"
	grpc_testing "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/metadata"
	grpcStatus "google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

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
		mockStream.EXPECT().Trailer().Return(metadata.MD{})

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
			mockStream.EXPECT().Trailer().Return(metadata.MD{})

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
			mockStream.EXPECT().Trailer().Return(metadata.MD{})

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
		mockStream.EXPECT().Trailer().Return(metadata.MD{})

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
			mockStream.EXPECT().Trailer().Return(metadata.MD{})

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
				mockStream.EXPECT().Trailer().Return(metadata.MD{})

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
				mockStream.EXPECT().Trailer().Return(metadata.MD{})

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
		mockStream.EXPECT().Trailer().Return(metadata.MD{})

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
		mockStream.EXPECT().Trailer().Return(metadata.MD{})

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
		require.False(t, canceled, "operation errors must not change the stream lifecycle")
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
		xtest.TestManyTimes(t, func(t testing.TB) {
			fake := &fakeTrailerStream{
				recv: func(m any) error {
					resp := m.(*Ydb_Query.ExecuteQueryResponsePart)
					resp.Status = Ydb.StatusIds_UNAVAILABLE

					return nil
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
}

func TestConn_NewStreamRealGRPCTrailers(t *testing.T) {
	for _, code := range []grpcCodes.Code{grpcCodes.OK, grpcCodes.Unavailable, grpcCodes.Canceled} {
		t.Run(code.String(), func(t *testing.T) {
			trailer := metadata.Pairs("x-ydb-server-hints", "session-close")
			listener := bufconn.Listen(1 << 20)
			t.Cleanup(func() { _ = listener.Close() })
			server := grpc.NewServer()
			grpc_testing.RegisterTestServiceServer(server, &trailerTestService{
				trailer:       trailer,
				err:           grpcStatus.Error(code, "stream finished"),
				waitForCancel: code == grpcCodes.Canceled,
			})
			t.Cleanup(server.Stop)
			go func() { _ = server.Serve(listener) }()

			rawConn, err := grpc.NewClient("passthrough:///trailer-test",
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
					return listener.DialContext(ctx)
				}),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			require.NoError(t, err)
			t.Cleanup(func() { _ = rawConn.Close() })
			finished := make(chan struct{})
			parentConn := newConn(endpoint.New("trailer-test"), &mockConfig{
				dialTimeout: 5 * time.Second,
				driverTrace: &trace.Driver{
					OnConnStreamFinish: func(trace.DriverConnStreamFinishInfo) {
						close(finished)
					},
				},
			})
			parentConn.grpcConn = rawConn

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			streamCtx, cancelStream := context.WithCancel(ctx)
			defer cancelStream()
			trailers := make(chan metadata.MD, 1)
			var callbacks atomic.Int32
			streamCtx = meta.WithTrailerCallback(streamCtx, func(md metadata.MD) {
				callbacks.Add(1)
				trailers <- md
			})
			var callerTrailer metadata.MD
			client := grpc_testing.NewTestServiceClient(parentConn)
			stream, err := client.StreamingOutputCall(streamCtx,
				&grpc_testing.StreamingOutputCallRequest{}, grpc.Trailer(&callerTrailer),
			)
			require.NoError(t, err)
			_, err = stream.Recv()
			require.NoError(t, err)
			if code == grpcCodes.Canceled {
				cancelStream()
			}
			_, err = stream.Recv()
			if code == grpcCodes.OK {
				require.ErrorIs(t, err, io.EOF)
			} else {
				require.True(t, xerrors.IsTransportError(err, code))
			}
			select {
			case <-finished:
			case <-ctx.Done():
				t.Fatal("stream finish callback did not run")
			}
			if code == grpcCodes.Canceled {
				require.Zero(t, callbacks.Load())
				require.Empty(t, callerTrailer)
				require.Empty(t, stream.Trailer())

				return
			}
			select {
			case md := <-trailers:
				require.Equal(t, trailer, md)
				md["x-ydb-server-hints"][0] = "changed in place"
				md.Set("x-ydb-server-hints", "changed")
			case <-ctx.Done():
				t.Fatal("trailer callback did not run")
			}
			cancelStream()
			require.EqualValues(t, 1, callbacks.Load())
			require.Equal(t, trailer, callerTrailer)
			require.Equal(t, trailer, stream.Trailer())
		})
	}
}

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

type trailerTestService struct {
	grpc_testing.UnimplementedTestServiceServer

	trailer       metadata.MD
	err           error
	waitForCancel bool
}

func (s *trailerTestService) StreamingOutputCall(
	_ *grpc_testing.StreamingOutputCallRequest,
	stream grpc.ServerStreamingServer[grpc_testing.StreamingOutputCallResponse],
) error {
	stream.SetTrailer(s.trailer)
	if err := stream.Send(&grpc_testing.StreamingOutputCallResponse{}); err != nil {
		return err
	}
	if s.waitForCancel {
		<-stream.Context().Done()

		return stream.Context().Err()
	}

	return s.err
}
