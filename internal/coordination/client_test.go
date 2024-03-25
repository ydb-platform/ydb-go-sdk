package coordination

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Coordination"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"go.uber.org/mock/gomock"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func TestClient_CreateNode(t *testing.T) {
	t.Run("HappyWay", func(t *testing.T) {
		xtest.TestManyTimes(t, func(t testing.TB) {
			ctx := xtest.Context(t)
			ctrl := gomock.NewController(t)
			client := NewMockCoordinationServiceClient(ctrl)
			client.EXPECT().CreateNode(gomock.Any(), gomock.Any()).Return(&Ydb_Coordination.CreateNodeResponse{
				Operation: &Ydb_Operations.Operation{
					Ready:  true,
					Status: Ydb.StatusIds_SUCCESS,
				},
			}, nil)
			err := createNode(ctx, client, &Ydb_Coordination.CreateNodeRequest{})
			require.NoError(t, err)
		}, xtest.StopAfter(time.Second))
	})
	t.Run("TransportError", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		client := NewMockCoordinationServiceClient(ctrl)
		client.EXPECT().CreateNode(gomock.Any(), gomock.Any()).Return(nil,
			xerrors.Transport(grpcStatus.Error(grpcCodes.Unavailable, "")),
		)
		err := createNode(ctx, client, &Ydb_Coordination.CreateNodeRequest{})
		require.True(t, xerrors.IsTransportError(err, grpcCodes.Unavailable))
	})
	t.Run("OperationError", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		client := NewMockCoordinationServiceClient(ctrl)
		client.EXPECT().CreateNode(gomock.Any(), gomock.Any()).Return(nil,
			xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE)),
		)
		err := createNode(ctx, client, &Ydb_Coordination.CreateNodeRequest{})
		require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_UNAVAILABLE))
	})
}
