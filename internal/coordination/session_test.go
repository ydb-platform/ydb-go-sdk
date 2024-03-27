package coordination

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Coordination"
	"go.uber.org/mock/gomock"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestNewSessionStream(t *testing.T) {
	t.Run("HappyWay", func(t *testing.T) {
		t.Run("First", func(t *testing.T) {
			t.Run("Message", func(t *testing.T) {
				t.Run("Started", func(t *testing.T) {
					ctx := xtest.Context(t)
					ctrl := gomock.NewController(t)
					client := NewMockCoordinationServiceClient(ctrl)
					sessionStream := NewMockCoordinationService_SessionClient(ctrl)
					sessionStream.EXPECT().Recv().Return(&Ydb_Coordination.SessionResponse{
						Response: &Ydb_Coordination.SessionResponse_SessionStarted_{
							SessionStarted: &Ydb_Coordination.SessionResponse_SessionStarted{
								SessionId:     123456789,
								TimeoutMillis: 987654321,
							},
						},
					}, nil)
					client.EXPECT().Session(gomock.Any()).Return(sessionStream, nil)
					s, err := newSessionStream(ctx, client, &trace.Coordination{})
					require.NoError(t, err)
					require.NotNil(t, s)
					require.EqualValues(t, 123456789, s.sessionID)
				})
				t.Run("Ping", func(t *testing.T) {
					ctx := xtest.Context(t)
					ctrl := gomock.NewController(t)
					client := NewMockCoordinationServiceClient(ctrl)
					sessionStream := NewMockCoordinationService_SessionClient(ctrl)
					sessionStream.EXPECT().Recv().Return(&Ydb_Coordination.SessionResponse{
						Response: &Ydb_Coordination.SessionResponse_Ping{
							Ping: &Ydb_Coordination.SessionResponse_PingPong{
								Opaque: 123123123,
							},
						},
					}, nil)
					sessionStream.EXPECT().Send(&Ydb_Coordination.SessionRequest{
						Request: &Ydb_Coordination.SessionRequest_Pong{
							Pong: &Ydb_Coordination.SessionRequest_PingPong{
								Opaque: 123123123,
							},
						},
					}).Return(nil)
					sessionStream.EXPECT().Recv().Return(&Ydb_Coordination.SessionResponse{
						Response: &Ydb_Coordination.SessionResponse_SessionStarted_{
							SessionStarted: &Ydb_Coordination.SessionResponse_SessionStarted{
								SessionId:     123456789,
								TimeoutMillis: 987654321,
							},
						},
					}, nil)
					client.EXPECT().Session(gomock.Any()).Return(sessionStream, nil)
					s, err := newSessionStream(ctx, client, &trace.Coordination{})
					require.NoError(t, err)
					require.NotNil(t, s)
					require.EqualValues(t, 123456789, s.sessionID)
				})
			})
		})
	})
	t.Run("TransportError", func(t *testing.T) {
		t.Run("On", func(t *testing.T) {
			t.Run("NewStream", func(t *testing.T) {
				ctx := xtest.Context(t)
				ctrl := gomock.NewController(t)
				client := NewMockCoordinationServiceClient(ctrl)
				sessionStream := NewMockCoordinationService_SessionClient(ctrl)
				sessionStream.EXPECT().Recv().Return(nil,
					xerrors.Transport(grpcStatus.Error(grpcCodes.Unavailable, "")),
				)
				client.EXPECT().Session(gomock.Any()).Return(sessionStream, nil)
				s, err := newSessionStream(ctx, client, &trace.Coordination{})
				require.Error(t, err)
				require.True(t, xerrors.IsTransportError(err, grpcCodes.Unavailable))
				require.Nil(t, s)
			})
			t.Run("Recv", func(t *testing.T) {
				ctx := xtest.Context(t)
				ctrl := gomock.NewController(t)
				client := NewMockCoordinationServiceClient(ctrl)
				client.EXPECT().Session(gomock.Any()).Return(nil,
					xerrors.Transport(grpcStatus.Error(grpcCodes.ResourceExhausted, "")),
				)
				s, err := newSessionStream(ctx, client, &trace.Coordination{})
				require.True(t, xerrors.IsTransportError(err, grpcCodes.ResourceExhausted))
				require.Nil(t, s)
			})
		})
	})
	t.Run("OperationError", func(t *testing.T) {
		t.Run("On", func(t *testing.T) {
			t.Run("NewStream", func(t *testing.T) {
				ctx := xtest.Context(t)
				ctrl := gomock.NewController(t)
				client := NewMockCoordinationServiceClient(ctrl)
				client.EXPECT().Session(gomock.Any()).Return(
					nil, xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_ABORTED)),
				)
				s, err := newSessionStream(ctx, client, &trace.Coordination{})
				require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_ABORTED))
				require.Nil(t, s)
			})
			t.Run("Recv", func(t *testing.T) {
				ctx := xtest.Context(t)
				ctrl := gomock.NewController(t)
				client := NewMockCoordinationServiceClient(ctrl)
				sessionStream := NewMockCoordinationService_SessionClient(ctrl)
				sessionStream.EXPECT().Recv().Return(&Ydb_Coordination.SessionResponse{
					Response: &Ydb_Coordination.SessionResponse_Failure_{
						Failure: &Ydb_Coordination.SessionResponse_Failure{
							Status: Ydb.StatusIds_ABORTED,
						},
					},
				}, nil)
				client.EXPECT().Session(gomock.Any()).Return(sessionStream, nil)
				s, err := newSessionStream(ctx, client, &trace.Coordination{})
				require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_ABORTED))
				require.Nil(t, s)
			})
		})
	})
}

func TestCreateSession(t *testing.T) {
	ctx := xtest.Context(t)
	ctrl := gomock.NewController(t)
	client := NewMockCoordinationServiceClient(ctrl)
	sessionStream := NewMockCoordinationService_SessionClient(ctrl)
	sessionStream.EXPECT().Send(gomock.Cond(func(r any) bool {
		if r.(*Ydb_Coordination.SessionRequest).GetSessionStart().GetPath() != "/a/b/c" {
			return false
		}
		if string(r.(*Ydb_Coordination.SessionRequest).GetSessionStart().GetProtectionKey()) != string(protectionKey) {
			return false
		}

		return true
	})).Return(nil)
	sessionStream.EXPECT().Recv().Return(&Ydb_Coordination.SessionResponse{
		Response: &Ydb_Coordination.SessionResponse_SessionStarted_{
			SessionStarted: &Ydb_Coordination.SessionResponse_SessionStarted{
				SessionId: 1,
			},
		},
	}, nil)
	sessionStream.EXPECT().Recv().Return(&Ydb_Coordination.SessionResponse{
		Response: &Ydb_Coordination.SessionResponse_Ping{
			Ping: &Ydb_Coordination.SessionResponse_PingPong{
				Opaque: 15810133112199159104,
			},
		},
	}, nil)
	sessionStream.EXPECT().Send(&Ydb_Coordination.SessionRequest{
		Request: &Ydb_Coordination.SessionRequest_Pong{
			Pong: &Ydb_Coordination.SessionRequest_PingPong{
				Opaque: 15810133112199159104,
			},
		},
	}).Return(nil)
	client.EXPECT().Session(gomock.Any()).Return(sessionStream, nil)
	s, err := createSession(ctx, client, "/a/b/c")
	require.NoError(t, err)
	require.NotNil(t, s)
	require.EqualValues(t, 123456789, s.sessionID)
	s.Close(ctx)
}
