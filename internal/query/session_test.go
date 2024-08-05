package query

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"go.uber.org/mock/gomock"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestBegin(t *testing.T) {
	t.Run("HappyWay", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		client := NewMockQueryServiceClient(ctrl)
		client.EXPECT().BeginTransaction(gomock.Any(), gomock.Any()).Return(&Ydb_Query.BeginTransactionResponse{
			Status: Ydb.StatusIds_SUCCESS,
			TxMeta: &Ydb_Query.TransactionMeta{
				Id: "123",
			},
		}, nil)
		t.Log("begin")
		tx, err := begin(ctx, client, &Session{id: "123"}, query.TxSettings())
		require.NoError(t, err)
		require.Equal(t, "123", tx.ID())
	})
	t.Run("TransportError", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		client := NewMockQueryServiceClient(ctrl)
		client.EXPECT().BeginTransaction(gomock.Any(), gomock.Any()).Return(nil, grpcStatus.Error(grpcCodes.Unavailable, ""))
		t.Log("begin")
		_, err := begin(ctx, client, &Session{id: "123"}, query.TxSettings())
		require.Error(t, err)
		require.True(t, xerrors.IsTransportError(err, grpcCodes.Unavailable))
	})
	t.Run("OperationError", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		client := NewMockQueryServiceClient(ctrl)
		client.EXPECT().BeginTransaction(gomock.Any(), gomock.Any()).Return(nil,
			xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE)),
		)
		t.Log("begin")
		_, err := begin(ctx, client, &Session{id: "123"}, query.TxSettings())
		require.Error(t, err)
		require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_UNAVAILABLE))
	})
}

func TestCreateSession(t *testing.T) {
	trace := &trace.Query{
		OnSessionCreate: func(info trace.QuerySessionCreateStartInfo) func(info trace.QuerySessionCreateDoneInfo) {
			return func(info trace.QuerySessionCreateDoneInfo) {
				if info.Session != nil && info.Error != nil {
					panic("only one result from tuple may be not nil")
				}
			}
		},
	}
	t.Run("HappyWay", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		client := NewMockQueryServiceClient(ctrl)
		client.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.CreateSessionResponse{
			Status:    Ydb.StatusIds_SUCCESS,
			SessionId: "123",
		}, nil)
		attachStream := NewMockQueryService_AttachSessionClient(ctrl)
		attachStream.EXPECT().Recv().Return(&Ydb_Query.SessionState{
			Status: Ydb.StatusIds_SUCCESS,
		}, nil).AnyTimes()
		client.EXPECT().AttachSession(gomock.Any(), &Ydb_Query.AttachSessionRequest{
			SessionId: "123",
		}).Return(attachStream, nil)
		t.Log("createSession")
		require.NotPanics(t, func() {
			s, err := createSession(ctx, client, config.New(config.WithTrace(trace)))
			require.NoError(t, err)
			require.NotNil(t, s)
			require.Equal(t, "123", s.ID())
		})
	})
	t.Run("TransportError", func(t *testing.T) {
		t.Run("OnCreateSession", func(t *testing.T) {
			ctx := xtest.Context(t)
			ctrl := gomock.NewController(t)
			client := NewMockQueryServiceClient(ctrl)
			client.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(nil,
				xerrors.Transport(grpcStatus.Error(grpcCodes.Unavailable, "test")),
			)
			t.Log("createSession")
			require.NotPanics(t, func() {
				s, err := createSession(ctx, client, config.New(config.WithTrace(trace)))
				require.Error(t, err)
				require.Nil(t, s)
			})
		})
		t.Run("OnAttachStream", func(t *testing.T) {
			ctx := xtest.Context(t)
			ctrl := gomock.NewController(t)
			client := NewMockQueryServiceClient(ctrl)
			client.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.CreateSessionResponse{
				Status:    Ydb.StatusIds_SUCCESS,
				SessionId: "123",
			}, nil)
			client.EXPECT().AttachSession(gomock.Any(), &Ydb_Query.AttachSessionRequest{
				SessionId: "123",
			}).Return(nil, xerrors.Transport(grpcStatus.Error(grpcCodes.Unavailable, "test")))
			client.EXPECT().DeleteSession(gomock.Any(), &Ydb_Query.DeleteSessionRequest{
				SessionId: "123",
			}).Return(&Ydb_Query.DeleteSessionResponse{
				Status: Ydb.StatusIds_SUCCESS,
			}, nil)
			t.Log("createSession")
			require.NotPanics(t, func() {
				s, err := createSession(ctx, client, config.New(config.WithTrace(trace)))
				require.Error(t, err)
				require.Nil(t, s)
			})
		})
	})
	t.Run("OperationError", func(t *testing.T) {
		t.Run("OnCreateSession", func(t *testing.T) {
			ctx := xtest.Context(t)
			ctrl := gomock.NewController(t)
			client := NewMockQueryServiceClient(ctrl)
			client.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(nil,
				xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE)),
			)
			t.Log("createSession")
			require.NotPanics(t, func() {
				s, err := createSession(ctx, client, config.New(config.WithTrace(trace)))
				require.Error(t, err)
				require.Nil(t, s)
			})
		})
		t.Run("OnAttachStream", func(t *testing.T) {
			ctx := xtest.Context(t)
			ctrl := gomock.NewController(t)
			client := NewMockQueryServiceClient(ctrl)
			client.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.CreateSessionResponse{
				Status:    Ydb.StatusIds_SUCCESS,
				SessionId: "123",
			}, nil)
			client.EXPECT().AttachSession(gomock.Any(), &Ydb_Query.AttachSessionRequest{
				SessionId: "123",
			}).Return(nil, xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE)))
			client.EXPECT().DeleteSession(gomock.Any(), &Ydb_Query.DeleteSessionRequest{
				SessionId: "123",
			}).Return(&Ydb_Query.DeleteSessionResponse{
				Status: Ydb.StatusIds_SUCCESS,
			}, nil)
			t.Log("createSession")
			require.NotPanics(t, func() {
				s, err := createSession(ctx, client, config.New(config.WithTrace(trace)))
				require.Error(t, err)
				require.Nil(t, s)
			})
		})
	})
}
