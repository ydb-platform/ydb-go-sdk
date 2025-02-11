package query

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"go.uber.org/mock/gomock"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

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
		require.NotPanics(t, func() {
			s, err := createSession(ctx, client, WithTrace(trace))
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
			require.NotPanics(t, func() {
				s, err := createSession(ctx, client, WithTrace(trace))
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
			require.NotPanics(t, func() {
				s, err := createSession(ctx, client, WithTrace(trace))
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
			require.NotPanics(t, func() {
				s, err := createSession(ctx, client, WithTrace(trace))
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
			require.NotPanics(t, func() {
				s, err := createSession(ctx, client, WithTrace(trace))
				require.Error(t, err)
				require.Nil(t, s)
			})
		})
	})
}
