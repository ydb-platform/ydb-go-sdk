package discovery

import (
	"testing"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Discovery"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"go.uber.org/mock/gomock"
	grpcCodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func TestDiscover(t *testing.T) {
	t.Run("HappyWay", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		clock := clockwork.NewFakeClock()
		client := NewMockDiscoveryServiceClient(ctrl)
		client.EXPECT().ListEndpoints(gomock.Any(), &Ydb_Discovery.ListEndpointsRequest{
			Database: "test",
		}).Return(&Ydb_Discovery.ListEndpointsResponse{
			Operation: &Ydb_Operations.Operation{
				Ready:  true,
				Status: Ydb.StatusIds_SUCCESS,
				Result: xtest.Must(anypb.New(&Ydb_Discovery.ListEndpointsResult{
					Endpoints: []*Ydb_Discovery.EndpointInfo{
						{
							Address: "node1",
							Port:    1,
							Ssl:     true,
						},
						{
							Address:  "node2",
							Port:     2,
							Location: "AZ0",
							Ssl:      true,
						},
						{
							Address: "node3",
							Port:    3,
							Ssl:     false,
						},
						{
							Address:  "node4",
							Port:     4,
							Location: "AZ0",
							Ssl:      false,
						},
					},
					SelfLocation: "AZ0",
				})),
			},
		}, nil)
		endpoints, location, err := Discover(ctx, client, config.New(
			config.WithDatabase("test"),
			config.WithSecure(false),
			config.WithClock(clock),
		))
		require.NoError(t, err)
		require.EqualValues(t, "AZ0", location)
		require.EqualValues(t, []endpoint.Endpoint{
			endpoint.New("node3:3",
				endpoint.WithLocalDC(false),
				endpoint.WithLastUpdated(clock.Now()),
			),
			endpoint.New("node4:4",
				endpoint.WithLocalDC(true),
				endpoint.WithLocation("AZ0"),
				endpoint.WithLastUpdated(clock.Now()),
			),
		}, endpoints)
	})
	t.Run("TransportError", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		client := NewMockDiscoveryServiceClient(ctrl)
		client.EXPECT().ListEndpoints(gomock.Any(), &Ydb_Discovery.ListEndpointsRequest{
			Database: "test",
		}).Return(nil, xerrors.Transport(status.Error(grpcCodes.Unavailable, "")))
		endpoints, location, err := Discover(ctx, client, config.New(
			config.WithDatabase("test"),
		))
		require.Error(t, err)
		require.Empty(t, endpoints)
		require.Equal(t, "", location)
		require.True(t, xerrors.IsTransportError(err, grpcCodes.Unavailable))
	})
	t.Run("OperationError", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		client := NewMockDiscoveryServiceClient(ctrl)
		client.EXPECT().ListEndpoints(gomock.Any(), &Ydb_Discovery.ListEndpointsRequest{
			Database: "test",
		}).Return(&Ydb_Discovery.ListEndpointsResponse{
			Operation: &Ydb_Operations.Operation{
				Ready:  true,
				Status: Ydb.StatusIds_UNAVAILABLE,
			},
		}, nil)
		endpoints, location, err := Discover(ctx, client, config.New(
			config.WithDatabase("test"),
		))
		require.Error(t, err)
		require.Empty(t, endpoints)
		require.Equal(t, "", location)
		require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_UNAVAILABLE))
	})
	t.Run("WithAddressMutator", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		clock := clockwork.NewFakeClock()
		client := NewMockDiscoveryServiceClient(ctrl)
		client.EXPECT().ListEndpoints(gomock.Any(), &Ydb_Discovery.ListEndpointsRequest{
			Database: "test",
		}).Return(&Ydb_Discovery.ListEndpointsResponse{
			Operation: &Ydb_Operations.Operation{
				Ready:  true,
				Status: Ydb.StatusIds_SUCCESS,
				Result: xtest.Must(anypb.New(&Ydb_Discovery.ListEndpointsResult{
					Endpoints: []*Ydb_Discovery.EndpointInfo{
						{
							Address: "node1",
							Port:    1,
						},
						{
							Address:  "node2",
							Port:     2,
							Location: "AZ0",
						},
					},
					SelfLocation: "AZ0",
				})),
			},
		}, nil)
		endpoints, location, err := Discover(ctx, client, config.New(
			config.WithDatabase("test"),
			config.WithAddressMutator(func(address string) string {
				return "u-" + address
			}),
			config.WithClock(clock),
		))
		require.NoError(t, err)
		require.EqualValues(t, "AZ0", location)
		require.EqualValues(t, []endpoint.Endpoint{
			endpoint.New("u-node1:1",
				endpoint.WithLocalDC(false),
				endpoint.WithLastUpdated(clock.Now()),
			),
			endpoint.New("u-node2:2",
				endpoint.WithLocalDC(true),
				endpoint.WithLocation("AZ0"),
				endpoint.WithLastUpdated(clock.Now()),
			),
		}, endpoints)
	})
}
