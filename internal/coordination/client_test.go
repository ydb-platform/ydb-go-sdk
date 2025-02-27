package coordination

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Coordination"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"go.uber.org/mock/gomock"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
)

func TestCreateNode(t *testing.T) {
	t.Run("HappyWay", func(t *testing.T) {
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
		require.False(t, mustDeleteSession(err))
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
		require.False(t, mustDeleteSession(err))
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

func TestDescribeNodeRequest(t *testing.T) {
	for _, tt := range []struct {
		name            string
		path            string
		operationParams *Ydb_Operations.OperationParams
		request         *Ydb_Coordination.DescribeNodeRequest
	}{
		{
			name: xtest.CurrentFileLine(),
			path: "/a/b/c",
			operationParams: &Ydb_Operations.OperationParams{
				OperationMode: Ydb_Operations.OperationParams_SYNC,
			},
			request: &Ydb_Coordination.DescribeNodeRequest{
				Path: "/a/b/c",
				OperationParams: &Ydb_Operations.OperationParams{
					OperationMode: Ydb_Operations.OperationParams_SYNC,
				},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			request := describeNodeRequest(tt.path, tt.operationParams)
			require.Equal(t, xtest.ToJSON(tt.request), xtest.ToJSON(request))
		})
	}
}

func TestOperationParams(t *testing.T) {
	for _, tt := range []struct {
		name   string
		ctx    context.Context //nolint:containedctx
		config interface {
			OperationTimeout() time.Duration
			OperationCancelAfter() time.Duration
		}
		mode            operation.Mode
		operationParams *Ydb_Operations.OperationParams
	}{
		{
			name:   xtest.CurrentFileLine(),
			ctx:    context.Background(),
			config: config.New(config.WithOperationCancelAfter(time.Second), config.WithOperationTimeout(time.Second)),
			mode:   operation.ModeSync,
			operationParams: &Ydb_Operations.OperationParams{
				OperationMode:    Ydb_Operations.OperationParams_SYNC,
				OperationTimeout: durationpb.New(time.Second),
				CancelAfter:      durationpb.New(time.Second),
			},
		},
		{
			name:   xtest.CurrentFileLine(),
			ctx:    operation.WithCancelAfter(operation.WithTimeout(context.Background(), time.Second), time.Second),
			config: config.New(),
			mode:   operation.ModeSync,
			operationParams: &Ydb_Operations.OperationParams{
				OperationMode:    Ydb_Operations.OperationParams_SYNC,
				OperationTimeout: durationpb.New(time.Second),
				CancelAfter:      durationpb.New(time.Second),
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			params := operationParams(tt.ctx, tt.config, tt.mode)
			require.Equal(t, xtest.ToJSON(tt.operationParams), xtest.ToJSON(params))
		})
	}
}

func TestDescribeNode(t *testing.T) {
	t.Run("HappyWay", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		client := NewMockCoordinationServiceClient(ctrl)
		client.EXPECT().DescribeNode(gomock.Any(), gomock.Any()).Return(&Ydb_Coordination.DescribeNodeResponse{
			Operation: &Ydb_Operations.Operation{
				Ready:  true,
				Status: Ydb.StatusIds_SUCCESS,
				Result: func() *anypb.Any {
					result, err := anypb.New(&Ydb_Coordination.DescribeNodeResult{
						Self: &Ydb_Scheme.Entry{
							Name:  "/a/b/c",
							Owner: "root",
							Type:  Ydb_Scheme.Entry_COORDINATION_NODE,
						},
						Config: &Ydb_Coordination.Config{
							Path:                     "/a/b/c",
							SelfCheckPeriodMillis:    100,
							SessionGracePeriodMillis: 1000,
							ReadConsistencyMode:      Ydb_Coordination.ConsistencyMode_CONSISTENCY_MODE_STRICT,
							AttachConsistencyMode:    Ydb_Coordination.ConsistencyMode_CONSISTENCY_MODE_STRICT,
							RateLimiterCountersMode:  Ydb_Coordination.RateLimiterCountersMode_RATE_LIMITER_COUNTERS_MODE_AGGREGATED,
						},
					})
					require.NoError(t, err)

					return result
				}(),
			},
		}, nil)
		nodeScheme, nodeConfig, err := describeNode(ctx, client, &Ydb_Coordination.DescribeNodeRequest{
			Path:            "/a/b/c",
			OperationParams: nil,
		})
		require.NoError(t, err)
		require.Equal(t, xtest.ToJSON(&scheme.Entry{
			Name:  "/a/b/c",
			Owner: "root",
			Type:  scheme.EntryCoordinationNode,
		}), xtest.ToJSON(nodeScheme))
		require.Equal(t, xtest.ToJSON(coordination.NodeConfig{
			Path:                     "/a/b/c",
			SelfCheckPeriodMillis:    100,
			SessionGracePeriodMillis: 1000,
			ReadConsistencyMode:      coordination.ConsistencyModeStrict,
			AttachConsistencyMode:    coordination.ConsistencyModeStrict,
			RatelimiterCountersMode:  coordination.RatelimiterCountersModeAggregated,
		}), xtest.ToJSON(nodeConfig))
	})
	t.Run("TransportError", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		client := NewMockCoordinationServiceClient(ctrl)
		client.EXPECT().DescribeNode(gomock.Any(), gomock.Any()).Return(nil,
			xerrors.Transport(grpcStatus.Error(grpcCodes.Unavailable, "")),
		)
		nodeScheme, nodeConfig, err := describeNode(ctx, client, &Ydb_Coordination.DescribeNodeRequest{
			Path:            "/a/b/c",
			OperationParams: nil,
		})
		require.True(t, xerrors.IsTransportError(err, grpcCodes.Unavailable))
		require.Nil(t, nodeScheme)
		require.Nil(t, nodeConfig)
	})
	t.Run("OperationError", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		client := NewMockCoordinationServiceClient(ctrl)
		client.EXPECT().DescribeNode(gomock.Any(), gomock.Any()).Return(nil,
			xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE)),
		)
		nodeScheme, nodeConfig, err := describeNode(ctx, client, &Ydb_Coordination.DescribeNodeRequest{
			Path:            "/a/b/c",
			OperationParams: nil,
		})
		require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_UNAVAILABLE))
		require.Nil(t, nodeScheme)
		require.Nil(t, nodeConfig)
	})
}

func TestAlterNodeRequest(t *testing.T) {
	for _, tt := range []struct {
		name            string
		path            string
		config          coordination.NodeConfig
		operationParams *Ydb_Operations.OperationParams
		request         *Ydb_Coordination.AlterNodeRequest
	}{
		{
			name: xtest.CurrentFileLine(),
			path: "/a/b/c",
			config: coordination.NodeConfig{
				Path: "/a/b/c",
			},
			operationParams: &Ydb_Operations.OperationParams{
				OperationMode: Ydb_Operations.OperationParams_SYNC,
			},
			request: &Ydb_Coordination.AlterNodeRequest{
				Path: "/a/b/c",
				Config: &Ydb_Coordination.Config{
					Path: "/a/b/c",
				},
				OperationParams: &Ydb_Operations.OperationParams{
					OperationMode: Ydb_Operations.OperationParams_SYNC,
				},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			request := alterNodeRequest(tt.path, tt.config, tt.operationParams)
			require.Equal(t, xtest.ToJSON(tt.request), xtest.ToJSON(request))
		})
	}
}

func TestAlterNode(t *testing.T) {
	t.Run("HappyWay", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		client := NewMockCoordinationServiceClient(ctrl)
		client.EXPECT().AlterNode(gomock.Any(), gomock.Any()).Return(&Ydb_Coordination.AlterNodeResponse{
			Operation: &Ydb_Operations.Operation{
				Ready:  true,
				Status: Ydb.StatusIds_SUCCESS,
			},
		}, nil)
		err := alterNode(ctx, client, &Ydb_Coordination.AlterNodeRequest{})
		require.NoError(t, err)
	})
	t.Run("TransportError", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		client := NewMockCoordinationServiceClient(ctrl)
		client.EXPECT().AlterNode(gomock.Any(), gomock.Any()).Return(nil,
			xerrors.Transport(grpcStatus.Error(grpcCodes.ResourceExhausted, "")),
		)
		err := alterNode(ctx, client, &Ydb_Coordination.AlterNodeRequest{})
		require.True(t, xerrors.IsTransportError(err, grpcCodes.ResourceExhausted))
		require.False(t, mustDeleteSession(err))
	})
	t.Run("OperationError", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		client := NewMockCoordinationServiceClient(ctrl)
		client.EXPECT().AlterNode(gomock.Any(), gomock.Any()).Return(nil,
			xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE)),
		)
		err := alterNode(ctx, client, &Ydb_Coordination.AlterNodeRequest{})
		require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_UNAVAILABLE))
		require.False(t, mustDeleteSession(err))
	})
}

func TestDropNodeRequest(t *testing.T) {
	for _, tt := range []struct {
		name            string
		path            string
		operationParams *Ydb_Operations.OperationParams
		request         *Ydb_Coordination.DropNodeRequest
	}{
		{
			name: xtest.CurrentFileLine(),
			path: "/a/b/c",
			operationParams: &Ydb_Operations.OperationParams{
				OperationMode: Ydb_Operations.OperationParams_SYNC,
			},
			request: &Ydb_Coordination.DropNodeRequest{
				Path: "/a/b/c",
				OperationParams: &Ydb_Operations.OperationParams{
					OperationMode: Ydb_Operations.OperationParams_SYNC,
				},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			request := dropNodeRequest(tt.path, tt.operationParams)
			require.Equal(t, xtest.ToJSON(tt.request), xtest.ToJSON(request))
		})
	}
}

func TestDropNode(t *testing.T) {
	t.Run("HappyWay", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		client := NewMockCoordinationServiceClient(ctrl)
		client.EXPECT().DropNode(gomock.Any(), gomock.Any()).Return(&Ydb_Coordination.DropNodeResponse{
			Operation: &Ydb_Operations.Operation{
				Ready:  true,
				Status: Ydb.StatusIds_SUCCESS,
			},
		}, nil)
		err := dropNode(ctx, client, &Ydb_Coordination.DropNodeRequest{})
		require.NoError(t, err)
	})
	t.Run("TransportError", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		client := NewMockCoordinationServiceClient(ctrl)
		client.EXPECT().DropNode(gomock.Any(), gomock.Any()).Return(nil,
			xerrors.Transport(grpcStatus.Error(grpcCodes.ResourceExhausted, "")),
		)
		err := dropNode(ctx, client, &Ydb_Coordination.DropNodeRequest{})
		require.True(t, xerrors.IsTransportError(err, grpcCodes.ResourceExhausted))
		require.False(t, mustDeleteSession(err))
	})
	t.Run("OperationError", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		client := NewMockCoordinationServiceClient(ctrl)
		client.EXPECT().DropNode(gomock.Any(), gomock.Any()).Return(nil,
			xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE)),
		)
		err := dropNode(ctx, client, &Ydb_Coordination.DropNodeRequest{})
		require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_UNAVAILABLE))
		require.False(t, mustDeleteSession(err))
	})
}
