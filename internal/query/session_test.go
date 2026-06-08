package query

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"go.uber.org/mock/gomock"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	baseTx "github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// TestSessionBeginLazyTxDeadSession ensures that Begin returns BAD_SESSION
// for a lazy transaction when the session is no longer alive. This prevents
// the dead session from being silently reused inside a new transaction.
func TestSessionBeginLazyTxDeadSession(t *testing.T) {
	ctx := t.Context()

	// Create a session whose underlying Core reports IsAlive() == false
	// (simulates a session that was previously invalidated by BAD_SESSION).
	deadCore := &sessionControllerMock{
		id:     "dead-session",
		status: StatusError,
	}
	s := &Session{
		Core:   deadCore,
		trace:  &trace.Query{},
		lazyTx: true, // lazy-tx mode
	}

	// Begin should refuse to create a lazy transaction for a dead session.
	lazyCtx := baseTx.WithLazyTx(ctx, true)
	tx, err := s.Begin(lazyCtx, query.TxSettings(query.WithSerializableReadWrite()))
	require.Error(t, err)
	require.Nil(t, tx)
	require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_BAD_SESSION))
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
		ctx := t.Context()
		ctrl := gomock.NewController(t)
		client := NewMockQueryServiceClient(ctrl)
		client.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(Ydb_Query.CreateSessionResponse_builder{
			Status:    Ydb.StatusIds_SUCCESS,
			SessionId: "123",
		}.Build(), nil)
		attachStream := NewMockQueryService_AttachSessionClient(ctrl)
		stubAttachStreamContext(attachStream)
		attachStream.EXPECT().Recv().Return(Ydb_Query.SessionState_builder{
			Status: Ydb.StatusIds_SUCCESS,
		}.Build(), nil)
		attachStream.EXPECT().Recv().Return(nil, errSessionClosed).AnyTimes()
		client.EXPECT().AttachSession(gomock.Any(), Ydb_Query.AttachSessionRequest_builder{
			SessionId: "123",
		}.Build()).Return(attachStream, nil)
		require.NotPanics(t, func() {
			s, err := createSession(ctx, client, WithTrace(trace))
			require.NoError(t, err)
			require.NotNil(t, s)
			require.Equal(t, "123", s.ID())
		})
	})
	t.Run("TransportError", func(t *testing.T) {
		t.Run("OnCreateSession", func(t *testing.T) {
			ctx := t.Context()
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
			ctx := t.Context()
			ctrl := gomock.NewController(t)
			client := NewMockQueryServiceClient(ctrl)
			client.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(Ydb_Query.CreateSessionResponse_builder{
				Status:    Ydb.StatusIds_SUCCESS,
				SessionId: "123",
			}.Build(), nil)
			client.EXPECT().AttachSession(gomock.Any(), Ydb_Query.AttachSessionRequest_builder{
				SessionId: "123",
			}.Build()).Return(nil, xerrors.Transport(grpcStatus.Error(grpcCodes.Unavailable, "test")))
			client.EXPECT().DeleteSession(gomock.Any(), Ydb_Query.DeleteSessionRequest_builder{
				SessionId: "123",
			}.Build()).Return(Ydb_Query.DeleteSessionResponse_builder{
				Status: Ydb.StatusIds_SUCCESS,
			}.Build(), nil)
			require.NotPanics(t, func() {
				s, err := createSession(ctx, client, WithTrace(trace))
				require.Error(t, err)
				require.Nil(t, s)
			})
		})
	})
	t.Run("OperationError", func(t *testing.T) {
		t.Run("OnCreateSession", func(t *testing.T) {
			ctx := t.Context()
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
			ctx := t.Context()
			ctrl := gomock.NewController(t)
			client := NewMockQueryServiceClient(ctrl)
			client.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(Ydb_Query.CreateSessionResponse_builder{
				Status:    Ydb.StatusIds_SUCCESS,
				SessionId: "123",
			}.Build(), nil)
			client.EXPECT().AttachSession(gomock.Any(), Ydb_Query.AttachSessionRequest_builder{
				SessionId: "123",
			}.Build()).Return(nil, xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE)))
			client.EXPECT().DeleteSession(gomock.Any(), Ydb_Query.DeleteSessionRequest_builder{
				SessionId: "123",
			}.Build()).Return(Ydb_Query.DeleteSessionResponse_builder{
				Status: Ydb.StatusIds_SUCCESS,
			}.Build(), nil)
			require.NotPanics(t, func() {
				s, err := createSession(ctx, client, WithTrace(trace))
				require.Error(t, err)
				require.Nil(t, s)
			})
		})
	})
}
