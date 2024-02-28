package query

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"go.uber.org/mock/gomock"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func TestCreateSession(t *testing.T) {
	t.Run("HappyWay", func(t *testing.T) {
		xtest.TestManyTimes(t, func(t testing.TB) {
			ctx := xtest.Context(t)
			ctrl := gomock.NewController(t)
			attachStream := NewMockQueryService_AttachSessionClient(ctrl)
			attachStream.EXPECT().Recv().Return(&Ydb_Query.SessionState{
				Status: Ydb.StatusIds_SUCCESS,
			}, nil).AnyTimes()
			attachStream.EXPECT().CloseSend().Return(nil)
			service := NewMockQueryServiceClient(ctrl)
			service.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.CreateSessionResponse{
				Status:    Ydb.StatusIds_SUCCESS,
				SessionId: "test",
			}, nil)
			service.EXPECT().AttachSession(gomock.Any(), gomock.Any()).Return(attachStream, nil)
			t.Log("execute")
			attached := 0
			s, err := createSession(ctx, service, createSessionSettings{
				onAttach: func(id string) {
					attached++
				},
				onDetach: func(id string) {
					attached--
				},
			})
			require.NoError(t, err)
			require.EqualValues(t, "test", s.id)
			require.EqualValues(t, 1, attached)
			s.close()
			require.EqualValues(t, 0, attached)
		}, xtest.StopAfter(time.Second))
	})
	t.Run("TransportError", func(t *testing.T) {
		t.Run("OnCall", func(t *testing.T) {
			xtest.TestManyTimes(t, func(t testing.TB) {
				ctx := xtest.Context(t)
				ctrl := gomock.NewController(t)
				service := NewMockQueryServiceClient(ctrl)
				service.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(nil, grpcStatus.Error(grpcCodes.Unavailable, ""))
				t.Log("execute")
				_, err := createSession(ctx, service, createSessionSettings{})
				require.Error(t, err)
				require.True(t, xerrors.IsTransportError(err, grpcCodes.Unavailable))
			}, xtest.StopAfter(time.Second))
		})
		t.Run("OnAttach", func(t *testing.T) {
			xtest.TestManyTimes(t, func(t testing.TB) {
				ctx := xtest.Context(t)
				ctrl := gomock.NewController(t)
				service := NewMockQueryServiceClient(ctrl)
				service.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.CreateSessionResponse{
					Status:    Ydb.StatusIds_SUCCESS,
					SessionId: "test",
				}, nil)
				service.EXPECT().AttachSession(gomock.Any(), gomock.Any()).Return(nil, grpcStatus.Error(grpcCodes.Unavailable, ""))
				service.EXPECT().DeleteSession(gomock.Any(), gomock.Any()).Return(nil, grpcStatus.Error(grpcCodes.Unavailable, ""))
				t.Log("execute")
				_, err := createSession(ctx, service, createSessionSettings{})
				require.Error(t, err)
				require.True(t, xerrors.IsTransportError(err, grpcCodes.Unavailable))
			}, xtest.StopAfter(time.Second))
		})
		t.Run("OnRecv", func(t *testing.T) {
			xtest.TestManyTimes(t, func(t testing.TB) {
				ctx := xtest.Context(t)
				ctrl := gomock.NewController(t)
				attachStream := NewMockQueryService_AttachSessionClient(ctrl)
				attachStream.EXPECT().Recv().Return(nil, grpcStatus.Error(grpcCodes.Unavailable, "")).AnyTimes()
				attachStream.EXPECT().CloseSend().Return(nil)
				service := NewMockQueryServiceClient(ctrl)
				service.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.CreateSessionResponse{
					Status:    Ydb.StatusIds_SUCCESS,
					SessionId: "test",
				}, nil)
				service.EXPECT().AttachSession(gomock.Any(), gomock.Any()).Return(attachStream, nil)
				service.EXPECT().DeleteSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.DeleteSessionResponse{
					Status: Ydb.StatusIds_SUCCESS,
				}, nil)
				t.Log("execute")
				_, err := createSession(ctx, service, createSessionSettings{})
				require.Error(t, err)
				require.True(t, xerrors.IsTransportError(err, grpcCodes.Unavailable))
			}, xtest.StopAfter(time.Second))
		})
	})
	t.Run("OperationError", func(t *testing.T) {
		t.Run("OnCall", func(t *testing.T) {
			xtest.TestManyTimes(t, func(t testing.TB) {
				ctx := xtest.Context(t)
				ctrl := gomock.NewController(t)
				service := NewMockQueryServiceClient(ctrl)
				service.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.CreateSessionResponse{
					Status: Ydb.StatusIds_UNAVAILABLE,
				}, nil)
				t.Log("execute")
				_, err := createSession(ctx, service, createSessionSettings{})
				require.Error(t, err)
				require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_UNAVAILABLE))
			}, xtest.StopAfter(time.Second))
		})
		t.Run("OnRecv", func(t *testing.T) {
			xtest.TestManyTimes(t, func(t testing.TB) {
				ctx := xtest.Context(t)
				ctrl := gomock.NewController(t)
				attachStream := NewMockQueryService_AttachSessionClient(ctrl)
				attachStream.EXPECT().Recv().Return(&Ydb_Query.SessionState{
					Status: Ydb.StatusIds_UNAVAILABLE,
				}, nil).AnyTimes()
				attachStream.EXPECT().CloseSend().Return(nil)
				service := NewMockQueryServiceClient(ctrl)
				service.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.CreateSessionResponse{
					Status:    Ydb.StatusIds_SUCCESS,
					SessionId: "test",
				}, nil)
				service.EXPECT().AttachSession(gomock.Any(), gomock.Any()).Return(attachStream, nil)
				service.EXPECT().DeleteSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.DeleteSessionResponse{
					Status: Ydb.StatusIds_SUCCESS,
				}, nil)
				t.Log("execute")
				_, err := createSession(ctx, service, createSessionSettings{})
				require.Error(t, err)
				require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_UNAVAILABLE))
			}, xtest.StopAfter(time.Second))
		})
	})
}

func newTestSession() (*Session, error) {
	return &Session{
		close: func() {},
	}, nil
}

func newTestSessionWithClient(client Ydb_Query_V1.QueryServiceClient) (*Session, error) {
	return &Session{
		queryClient: client,
		close:       func() {},
	}, nil
}

func newTestPool(t testing.TB, createSession func(ctx context.Context) (*Session, error)) *stubPool {
	return newStubPool(
		createSession,
		func(ctx context.Context, s *Session) error {
			return s.Close(ctx)
		},
	)
}

func TestDo(t *testing.T) {
	ctx := xtest.Context(t)
	t.Run("HappyWay", func(t *testing.T) {
		err := do(ctx, newTestPool(t, func(ctx context.Context) (*Session, error) {
			return newTestSession()
		}), func(ctx context.Context, s query.Session) error {
			return nil
		}, &query.DoOptions{})
		require.NoError(t, err)
	})
	t.Run("RetryableError", func(t *testing.T) {
		counter := 0
		err := do(ctx, newTestPool(t, func(ctx context.Context) (*Session, error) {
			return newTestSession()
		}), func(ctx context.Context, s query.Session) error {
			counter++
			if counter < 10 {
				return xerrors.Retryable(errors.New(""))
			}

			return nil
		}, &query.DoOptions{})
		require.NoError(t, err)
		require.Equal(t, 10, counter)
	})
}

func TestDoTx(t *testing.T) {
	ctx := xtest.Context(t)
	t.Run("HappyWay", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		client := NewMockQueryServiceClient(ctrl)
		client.EXPECT().BeginTransaction(gomock.Any(), gomock.Any()).Return(&Ydb_Query.BeginTransactionResponse{
			Status: Ydb.StatusIds_SUCCESS,
		}, nil)
		client.EXPECT().CommitTransaction(gomock.Any(), gomock.Any()).Return(&Ydb_Query.CommitTransactionResponse{
			Status: Ydb.StatusIds_SUCCESS,
		}, nil)
		err := doTx(ctx, newTestPool(t, func(ctx context.Context) (*Session, error) {
			return newTestSessionWithClient(client)
		}), func(ctx context.Context, tx query.TxActor) error {
			return nil
		}, &query.DoTxOptions{})
		require.NoError(t, err)
	})
	t.Run("RetryableError", func(t *testing.T) {
		counter := 0
		ctrl := gomock.NewController(t)
		client := NewMockQueryServiceClient(ctrl)
		client.EXPECT().BeginTransaction(gomock.Any(), gomock.Any()).Return(&Ydb_Query.BeginTransactionResponse{
			Status: Ydb.StatusIds_SUCCESS,
		}, nil).AnyTimes()
		client.EXPECT().RollbackTransaction(gomock.Any(), gomock.Any()).Return(&Ydb_Query.RollbackTransactionResponse{
			Status: Ydb.StatusIds_SUCCESS,
		}, nil).AnyTimes()
		client.EXPECT().CommitTransaction(gomock.Any(), gomock.Any()).Return(&Ydb_Query.CommitTransactionResponse{
			Status: Ydb.StatusIds_SUCCESS,
		}, nil).AnyTimes()
		err := doTx(ctx, newTestPool(t, func(ctx context.Context) (*Session, error) {
			return newTestSessionWithClient(client)
		}), func(ctx context.Context, tx query.TxActor) error {
			counter++
			if counter < 10 {
				return xerrors.Retryable(errors.New(""))
			}

			return nil
		}, &query.DoTxOptions{})
		require.NoError(t, err)
		require.Equal(t, 10, counter)
	})
}
