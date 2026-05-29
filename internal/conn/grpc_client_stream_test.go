package conn

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"go.uber.org/mock/gomock"
	grpcCodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

//go:generate mockgen -destination grpc_client_stream_mock_test.go --typed -package conn -write_package_comment=false google.golang.org/grpc ClientStream

func TestGrpcClientStream_Header(t *testing.T) {
	t.Run("ReturnsHeaderFromUnderlyingStream", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := NewMockClientStream(ctrl)

		expectedMD := metadata.MD{"key": []string{"value"}}
		mockStream.EXPECT().Header().Return(expectedMD, nil)

		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
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
		mockStream := NewMockClientStream(ctrl)

		expectedErr := fmt.Errorf("header error")
		mockStream.EXPECT().Header().Return(metadata.MD{}, expectedErr)

		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
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
		mockStream := NewMockClientStream(ctrl)

		expectedMD := metadata.MD{"trailer-key": []string{"trailer-value"}}
		mockStream.EXPECT().Trailer().Return(expectedMD)

		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
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
		mockStream := NewMockClientStream(ctrl)

		expectedCtx := context.WithValue(context.Background(), "key", "value") //nolint:revive,staticcheck
		mockStream.EXPECT().Context().Return(expectedCtx)

		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
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
		mockStream := NewMockClientStream(ctrl)

		mockStream.EXPECT().CloseSend().Return(nil)

		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			streamCtx:  context.Background(),
			wrapping:   true,
			traceID:    "test-trace-id",
		}

		err := s.CloseSend()
		require.NoError(t, err)
	})

	t.Run("ContextError", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := NewMockClientStream(ctrl)

		mockStream.EXPECT().CloseSend().Return(context.Canceled)

		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			streamCtx:  context.Background(),
			wrapping:   true,
			traceID:    "test-trace-id",
		}

		err := s.CloseSend()
		require.Error(t, err)
		require.True(t, xerrors.IsContextError(err))
	})

	t.Run("TransportErrorWithWrapping", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := NewMockClientStream(ctrl)

		mockStream.EXPECT().CloseSend().Return(fmt.Errorf("transport error"))

		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
		}
		e := endpoint.New("test-endpoint:2135", endpoint.WithID(123))
		parentConn := newConn(e, config)

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			streamCtx:  context.Background(),
			wrapping:   true,
			traceID:    "test-trace-id",
		}

		err := s.CloseSend()
		require.Error(t, err)
		require.True(t, xerrors.IsTransportError(err))
	})

	t.Run("ErrorWithoutWrapping", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := NewMockClientStream(ctrl)

		expectedErr := fmt.Errorf("raw error")
		mockStream.EXPECT().CloseSend().Return(expectedErr)

		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			streamCtx:  context.Background(),
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
		mockStream := NewMockClientStream(ctrl)

		msg := &Ydb_Query.ExecuteQueryRequest{}
		mockStream.EXPECT().SendMsg(msg).Return(nil)

		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			streamCtx:  context.Background(),
			wrapping:   true,
			sentMark:   &modificationMark{},
		}

		err := s.SendMsg(msg)
		require.NoError(t, err)
	})

	t.Run("ContextError", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := NewMockClientStream(ctrl)

		msg := &Ydb_Query.ExecuteQueryRequest{}
		mockStream.EXPECT().SendMsg(msg).Return(context.DeadlineExceeded)

		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			streamCtx:  context.Background(),
			wrapping:   true,
			sentMark:   &modificationMark{},
		}

		err := s.SendMsg(msg)
		require.Error(t, err)
		require.True(t, xerrors.IsContextError(err))
	})

	t.Run("TransportErrorRetryable", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := NewMockClientStream(ctrl)

		msg := &Ydb_Query.ExecuteQueryRequest{}
		mockStream.EXPECT().SendMsg(msg).Return(grpcStatus.Error(grpcCodes.Unavailable, "unavailable"))

		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			streamCtx:  context.Background(),
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
		mockStream := NewMockClientStream(ctrl)

		msg := &Ydb_Query.ExecuteQueryRequest{}
		mockStream.EXPECT().SendMsg(msg).Return(grpcStatus.Error(grpcCodes.Unavailable, "unavailable"))

		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
		}
		e := endpoint.New("test-endpoint:2135", endpoint.WithID(123))
		parentConn := newConn(e, config)

		mark := &modificationMark{}
		mark.markDirty()

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			streamCtx:  context.Background(),
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
		mockStream := NewMockClientStream(ctrl)

		msg := &Ydb_Query.ExecuteQueryRequest{}
		expectedErr := fmt.Errorf("raw error")
		mockStream.EXPECT().SendMsg(msg).Return(expectedErr)

		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			streamCtx:  context.Background(),
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
		mockStream := NewMockClientStream(ctrl)

		msg := &Ydb_Query.ExecuteQueryResponsePart{}
		mockStream.EXPECT().RecvMsg(msg).DoAndReturn(func(m interface{}) error {
			resp := m.(*Ydb_Query.ExecuteQueryResponsePart)
			resp.Status = Ydb.StatusIds_SUCCESS

			return nil
		})

		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			streamCtx:  context.Background(),
			wrapping:   true,
			sentMark:   &modificationMark{},
		}

		err := s.RecvMsg(msg)
		require.NoError(t, err)
	})

	t.Run("EOFError", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := NewMockClientStream(ctrl)

		msg := &Ydb_Query.ExecuteQueryResponsePart{}
		mockStream.EXPECT().RecvMsg(msg).Return(io.EOF)
		mockStream.EXPECT().Trailer().Return(metadata.MD{})

		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			streamCtx:  context.Background(),
			wrapping:   true,
			sentMark:   &modificationMark{},
		}

		err := s.RecvMsg(msg)
		require.Equal(t, io.EOF, err)
	})

	t.Run("ContextError", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := NewMockClientStream(ctrl)

		msg := &Ydb_Query.ExecuteQueryResponsePart{}
		mockStream.EXPECT().RecvMsg(msg).Return(context.Canceled)
		mockStream.EXPECT().Trailer().Return(metadata.MD{})

		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			streamCtx:  context.Background(),
			wrapping:   true,
			sentMark:   &modificationMark{},
		}

		err := s.RecvMsg(msg)
		require.Error(t, err)
		require.True(t, xerrors.IsContextError(err))
	})

	t.Run("TransportErrorRetryable", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := NewMockClientStream(ctrl)

		msg := &Ydb_Query.ExecuteQueryResponsePart{}
		mockStream.EXPECT().RecvMsg(msg).Return(grpcStatus.Error(grpcCodes.Unavailable, "unavailable"))
		mockStream.EXPECT().Trailer().Return(metadata.MD{})

		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			streamCtx:  context.Background(),
			wrapping:   true,
			traceID:    "test-trace-id",
			sentMark:   &modificationMark{},
		}

		err := s.RecvMsg(msg)
		require.Error(t, err)
		require.True(t, xerrors.IsTransportError(err))
		require.True(t, xerrors.IsRetryableError(err))
	})

	t.Run("TransportErrorNonRetryable", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := NewMockClientStream(ctrl)

		msg := &Ydb_Query.ExecuteQueryResponsePart{}
		mockStream.EXPECT().RecvMsg(msg).Return(grpcStatus.Error(grpcCodes.Unavailable, "unavailable"))
		mockStream.EXPECT().Trailer().Return(metadata.MD{})

		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
		}
		e := endpoint.New("test-endpoint:2135", endpoint.WithID(123))
		parentConn := newConn(e, config)

		mark := &modificationMark{}
		mark.markDirty()

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			streamCtx:  context.Background(),
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
		mockStream := NewMockClientStream(ctrl)

		msg := &Ydb_Query.ExecuteQueryResponsePart{}
		expectedErr := fmt.Errorf("raw error")
		mockStream.EXPECT().RecvMsg(msg).Return(expectedErr)
		mockStream.EXPECT().Trailer().Return(metadata.MD{})

		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			streamCtx:  context.Background(),
			wrapping:   false,
			sentMark:   &modificationMark{},
		}

		err := s.RecvMsg(msg)
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
	})

	t.Run("OperationError", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := NewMockClientStream(ctrl)

		msg := &Ydb_Query.ExecuteQueryResponsePart{}
		mockStream.EXPECT().RecvMsg(msg).DoAndReturn(func(m interface{}) error {
			resp := m.(*Ydb_Query.ExecuteQueryResponsePart)
			resp.Status = Ydb.StatusIds_UNAVAILABLE

			return nil
		})
		mockStream.EXPECT().Trailer().Return(metadata.MD{})

		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
		}
		e := endpoint.New("test-endpoint:2135", endpoint.WithID(123))
		parentConn := newConn(e, config)

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			streamCtx:  context.Background(),
			wrapping:   true,
			sentMark:   &modificationMark{},
		}

		err := s.RecvMsg(msg)
		require.Error(t, err)
		require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_UNAVAILABLE))
	})

	t.Run("OperationErrorWithoutWrapping", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := NewMockClientStream(ctrl)

		msg := &Ydb_Query.ExecuteQueryResponsePart{}
		mockStream.EXPECT().RecvMsg(msg).DoAndReturn(func(m interface{}) error {
			resp := m.(*Ydb_Query.ExecuteQueryResponsePart)
			resp.Status = Ydb.StatusIds_UNAVAILABLE

			return nil
		})

		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		s := &grpcClientStream{
			parentConn: parentConn,
			stream:     mockStream,
			streamCtx:  context.Background(),
			wrapping:   false,
			sentMark:   &modificationMark{},
		}

		err := s.RecvMsg(msg)
		require.NoError(t, err)
	})
}

func TestGrpcClientStream_Finish(t *testing.T) {
	t.Run("CallsCancelOnFinish", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := NewMockClientStream(ctrl)

		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
			driverTrace:   &trace.Driver{},
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		ctx, cancel := context.WithCancel(context.Background())
		called := false
		wrappedCancel := func() {
			called = true
			cancel()
		}

		s := &grpcClientStream{
			parentConn:   parentConn,
			stream:       mockStream,
			streamCtx:    ctx,
			streamCancel: wrappedCancel,
		}

		s.finish(nil)
		require.True(t, called)
	})

	t.Run("FinishWithError", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStream := NewMockClientStream(ctrl)

		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
			driverTrace:   &trace.Driver{},
		}
		e := endpoint.New("test-endpoint:2135")
		parentConn := newConn(e, config)

		ctx, cancel := context.WithCancel(context.Background())

		s := &grpcClientStream{
			parentConn:   parentConn,
			stream:       mockStream,
			streamCtx:    ctx,
			streamCancel: cancel,
		}

		testErr := fmt.Errorf("test error")
		s.finish(testErr)
		// Should not panic
	})
}
