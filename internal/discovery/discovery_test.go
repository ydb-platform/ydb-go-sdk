package discovery

import (
	"testing"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Bridge"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Discovery"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	xtest "github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
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
							Address:        "node3",
							Port:           3,
							Ssl:            false,
							BridgePileName: "pile-a",
						},
						{
							Address:        "node4",
							Port:           4,
							Location:       "AZ0",
							Ssl:            false,
							BridgePileName: "pile-b",
						},
						{
							Address:        "node5",
							Port:           5,
							Ssl:            false,
							BridgePileName: "missing-pile",
						},
						{
							Address: "node6",
							Port:    6,
							Ssl:     false,
						},
					},
					SelfLocation: "AZ0",
					PileStates: []*Ydb_Bridge.PileState{
						{PileName: "pile-a", State: Ydb_Bridge.PileState_PRIMARY},
						{PileName: "pile-b", State: Ydb_Bridge.PileState_SYNCHRONIZED},
					},
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
				endpoint.WithMetadata(endpoint.Metadata{BridgePileState: endpoint.PileStatePrimary}),
				endpoint.WithLastUpdated(clock.Now()),
			),
			endpoint.New("node4:4",
				endpoint.WithLocation("AZ0"),
				endpoint.WithMetadata(endpoint.Metadata{
					LocalDC:         true,
					BridgePileState: endpoint.PileStateSynchronized,
				}),
				endpoint.WithLastUpdated(clock.Now()),
			),
			endpoint.New("node5:5",
				endpoint.WithMetadata(endpoint.Metadata{BridgePileState: endpoint.PileStateUnknown}),
				endpoint.WithLastUpdated(clock.Now()),
			),
			endpoint.New("node6:6",
				endpoint.WithMetadata(endpoint.Metadata{BridgePileState: endpoint.PileStateUnknown}),
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
		}).Return(nil, status.Error(grpcCodes.Unavailable, ""))
		endpoints, location, err := Discover(ctx, client, config.New(
			config.WithDatabase("test"),
		))
		require.Error(t, err)
		require.Empty(t, endpoints)
		require.Equal(t, "", location)
		require.True(t, xerrors.IsTransportError(err, grpcCodes.Unavailable))
		require.True(t, retry.Check(err).MustRetry(true), "must retry")
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

func TestBridgePileState(t *testing.T) {
	tests := []struct {
		proto    Ydb_Bridge.PileState_State
		expected endpoint.PileState
	}{
		{Ydb_Bridge.PileState_UNSPECIFIED, endpoint.PileStateUnknown},
		{Ydb_Bridge.PileState_PRIMARY, endpoint.PileStatePrimary},
		{Ydb_Bridge.PileState_PROMOTED, endpoint.PileStatePromoted},
		{Ydb_Bridge.PileState_SYNCHRONIZED, endpoint.PileStateSynchronized},
		{Ydb_Bridge.PileState_NOT_SYNCHRONIZED, endpoint.PileStateNotSynchronized},
		{Ydb_Bridge.PileState_SUSPENDED, endpoint.PileStateSuspended},
		{Ydb_Bridge.PileState_DISCONNECTED, endpoint.PileStateDisconnected},
		{Ydb_Bridge.PileState_State(100), endpoint.PileStateUnknown},
	}

	for _, test := range tests {
		require.Equal(t, test.expected, bridgePileState(test.proto))
	}
}

func TestClientCloseSkipsConnWithoutIOCloser(t *testing.T) {
	client := &Client{
		cc: struct{ grpc.ClientConnInterface }{},
	}

	require.NoError(t, client.Close(t.Context()))
}

func TestBridgePileState(t *testing.T) {
	tests := []struct {
		proto    Ydb_Bridge.PileState_State
		expected endpoint.PileState
	}{
		{Ydb_Bridge.PileState_UNSPECIFIED, endpoint.PileStateUnknown},
		{Ydb_Bridge.PileState_PRIMARY, endpoint.PileStatePrimary},
		{Ydb_Bridge.PileState_PROMOTED, endpoint.PileStatePromoted},
		{Ydb_Bridge.PileState_SYNCHRONIZED, endpoint.PileStateSynchronized},
		{Ydb_Bridge.PileState_NOT_SYNCHRONIZED, endpoint.PileStateNotSynchronized},
		{Ydb_Bridge.PileState_SUSPENDED, endpoint.PileStateSuspended},
		{Ydb_Bridge.PileState_DISCONNECTED, endpoint.PileStateDisconnected},
		{Ydb_Bridge.PileState_State(100), endpoint.PileStateUnknown},
	}

	for _, test := range tests {
		require.Equal(t, test.expected, bridgePileState(test.proto))
	}
}
