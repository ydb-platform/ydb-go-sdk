package coordination

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Coordination"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"go.uber.org/mock/gomock"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
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
			xerrors.Transport(grpcStatus.Error(grpcCodes.ResourceExhausted, "")),
		)
		err := createNode(ctx, client, &Ydb_Coordination.CreateNodeRequest{})
		require.True(t, xerrors.IsTransportError(err, grpcCodes.ResourceExhausted))
		require.True(t, xerrors.IsRetryObjectValid(err))
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
		require.True(t, xerrors.IsRetryObjectValid(err))
	})
}

func TestCreateNodeRequest(t *testing.T) {
	for _, tt := range []struct {
		name            string
		path            string
		config          coordination.NodeConfig
		operationParams *Ydb_Operations.OperationParams
		request         *Ydb_Coordination.CreateNodeRequest
	}{
		{
			name: xtest.CurrentFileLine(),
			path: "/abc",
			config: coordination.NodeConfig{
				Path: "/cde",
			},
			operationParams: operation.Params(context.Background(), time.Second, time.Second, operation.ModeSync),
			request: &Ydb_Coordination.CreateNodeRequest{
				Path: "/abc",
				Config: &Ydb_Coordination.Config{
					Path: "/cde",
				},
				OperationParams: &Ydb_Operations.OperationParams{
					OperationMode:    Ydb_Operations.OperationParams_SYNC,
					OperationTimeout: durationpb.New(time.Second),
					CancelAfter:      durationpb.New(time.Second),
				},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			request := createNodeRequest(tt.path, tt.config, tt.operationParams)
			require.EqualValues(t, xtest.ToJSON(tt.request), xtest.ToJSON(request))
		})
	}
}
