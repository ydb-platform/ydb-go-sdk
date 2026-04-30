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
	t.Run("WithOnlyIPv6", func(t *testing.T) {
		// Discovery response contains a mix of endpoints:
		//   dualstack  - has both IPv4 and IPv6 resolved addresses -> IPv4 must be dropped
		//   v4only     - has only IPv4 resolved addresses -> must be skipped entirely
		//   v6only     - has only IPv6 resolved addresses -> must be kept as-is
		//   fqdnonly   - has neither resolved address -> must be kept as-is (DNS-resolved at dial time)
		//   filtered   - fails the SSL filter, must be skipped regardless of OnlyIPv6
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
							Address: "dualstack.ydb",
							Port:    1,
							Ssl:     true,
							IpV4:    []string{"10.0.0.1"},
							IpV6:    []string{"2001:db8::1"},
						},
						{
							Address: "v4only.ydb",
							Port:    2,
							Ssl:     true,
							IpV4:    []string{"10.0.0.2"},
						},
						{
							Address: "v6only.ydb",
							Port:    3,
							Ssl:     true,
							IpV6:    []string{"2001:db8::2"},
						},
						{
							Address: "fqdnonly.ydb",
							Port:    4,
							Ssl:     true,
						},
						{
							Address: "filtered.ydb",
							Port:    5,
							Ssl:     false,
							IpV6:    []string{"2001:db8::3"},
						},
					},
					SelfLocation: "AZ0",
				})),
			},
		}, nil)
		endpoints, location, err := Discover(ctx, client, config.New(
			config.WithDatabase("test"),
			config.WithSecure(true),
			config.WithOnlyIPv6(),
			config.WithClock(clock),
		))
		require.NoError(t, err)
		require.EqualValues(t, "AZ0", location)
		require.EqualValues(t, []endpoint.Endpoint{
			endpoint.New("dualstack.ydb:1",
				endpoint.WithLocalDC(false),
				endpoint.WithLastUpdated(clock.Now()),
				endpoint.WithIPV6([]string{"2001:db8::1"}),
			),
			endpoint.New("v6only.ydb:3",
				endpoint.WithLocalDC(false),
				endpoint.WithLastUpdated(clock.Now()),
				endpoint.WithIPV6([]string{"2001:db8::2"}),
			),
			endpoint.New("fqdnonly.ydb:4",
				endpoint.WithLocalDC(false),
				endpoint.WithLastUpdated(clock.Now()),
			),
		}, endpoints)

		for _, e := range endpoints {
			require.NotContains(t, e.Address(), "10.0.0.",
				"IPv6-only endpoint must not resolve to an IPv4 address: %s", e.Address(),
			)
		}
	})
	t.Run("WithOnlyIPv6DefaultOff", func(t *testing.T) {
		// Without WithOnlyIPv6 the behavior must stay unchanged:
		// IPv4 addresses (when present) are kept and are preferred by endpoint.Address().
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
							Address: "dualstack.ydb",
							Port:    1,
							Ssl:     true,
							IpV4:    []string{"10.0.0.1"},
							IpV6:    []string{"2001:db8::1"},
						},
						{
							Address: "v4only.ydb",
							Port:    2,
							Ssl:     true,
							IpV4:    []string{"10.0.0.2"},
						},
					},
					SelfLocation: "AZ0",
				})),
			},
		}, nil)
		endpoints, _, err := Discover(ctx, client, config.New(
			config.WithDatabase("test"),
			config.WithSecure(true),
			config.WithClock(clock),
		))
		require.NoError(t, err)
		require.EqualValues(t, []endpoint.Endpoint{
			endpoint.New("dualstack.ydb:1",
				endpoint.WithLocalDC(false),
				endpoint.WithLastUpdated(clock.Now()),
				endpoint.WithIPV4([]string{"10.0.0.1"}),
				endpoint.WithIPV6([]string{"2001:db8::1"}),
			),
			endpoint.New("v4only.ydb:2",
				endpoint.WithLocalDC(false),
				endpoint.WithLastUpdated(clock.Now()),
				endpoint.WithIPV4([]string{"10.0.0.2"}),
			),
		}, endpoints)
	})
}
