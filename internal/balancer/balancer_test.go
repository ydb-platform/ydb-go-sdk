package balancer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	grpcStatus "google.golang.org/grpc/status"

	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestEndpointsDiff(t *testing.T) {
	for _, tt := range []struct {
		newestEndpoints []trace.EndpointInfo
		previousConns   []trace.EndpointInfo
		nodes           []trace.EndpointInfo
		added           []trace.EndpointInfo
		dropped         []trace.EndpointInfo
	}{
		{
			newestEndpoints: []trace.EndpointInfo{
				&mock.Endpoint{AddressField: "1"},
				&mock.Endpoint{AddressField: "3"},
				&mock.Endpoint{AddressField: "2"},
				&mock.Endpoint{AddressField: "0"},
			},
			previousConns: []trace.EndpointInfo{
				&mock.Endpoint{AddressField: "2"},
				&mock.Endpoint{AddressField: "1"},
				&mock.Endpoint{AddressField: "0"},
				&mock.Endpoint{AddressField: "3"},
			},
			nodes: []trace.EndpointInfo{
				&mock.Endpoint{AddressField: "0"},
				&mock.Endpoint{AddressField: "1"},
				&mock.Endpoint{AddressField: "2"},
				&mock.Endpoint{AddressField: "3"},
			},
			added:   []trace.EndpointInfo{},
			dropped: []trace.EndpointInfo{},
		},
		{
			newestEndpoints: []trace.EndpointInfo{
				&mock.Endpoint{AddressField: "1"},
				&mock.Endpoint{AddressField: "3"},
				&mock.Endpoint{AddressField: "2"},
				&mock.Endpoint{AddressField: "0"},
			},
			previousConns: []trace.EndpointInfo{
				&mock.Endpoint{AddressField: "1"},
				&mock.Endpoint{AddressField: "0"},
				&mock.Endpoint{AddressField: "3"},
			},
			nodes: []trace.EndpointInfo{
				&mock.Endpoint{AddressField: "0"},
				&mock.Endpoint{AddressField: "1"},
				&mock.Endpoint{AddressField: "2"},
				&mock.Endpoint{AddressField: "3"},
			},
			added: []trace.EndpointInfo{
				&mock.Endpoint{AddressField: "2"},
			},
			dropped: []trace.EndpointInfo{},
		},
		{
			newestEndpoints: []trace.EndpointInfo{
				&mock.Endpoint{AddressField: "1"},
				&mock.Endpoint{AddressField: "3"},
				&mock.Endpoint{AddressField: "0"},
			},
			previousConns: []trace.EndpointInfo{
				&mock.Endpoint{AddressField: "1"},
				&mock.Endpoint{AddressField: "2"},
				&mock.Endpoint{AddressField: "0"},
				&mock.Endpoint{AddressField: "3"},
			},
			nodes: []trace.EndpointInfo{
				&mock.Endpoint{AddressField: "0"},
				&mock.Endpoint{AddressField: "1"},
				&mock.Endpoint{AddressField: "3"},
			},
			added: []trace.EndpointInfo{},
			dropped: []trace.EndpointInfo{
				&mock.Endpoint{AddressField: "2"},
			},
		},
		{
			newestEndpoints: []trace.EndpointInfo{
				&mock.Endpoint{AddressField: "1"},
				&mock.Endpoint{AddressField: "3"},
				&mock.Endpoint{AddressField: "0"},
			},
			previousConns: []trace.EndpointInfo{
				&mock.Endpoint{AddressField: "4"},
				&mock.Endpoint{AddressField: "7"},
				&mock.Endpoint{AddressField: "8"},
			},
			nodes: []trace.EndpointInfo{
				&mock.Endpoint{AddressField: "0"},
				&mock.Endpoint{AddressField: "1"},
				&mock.Endpoint{AddressField: "3"},
			},
			added: []trace.EndpointInfo{
				&mock.Endpoint{AddressField: "0"},
				&mock.Endpoint{AddressField: "1"},
				&mock.Endpoint{AddressField: "3"},
			},
			dropped: []trace.EndpointInfo{
				&mock.Endpoint{AddressField: "4"},
				&mock.Endpoint{AddressField: "7"},
				&mock.Endpoint{AddressField: "8"},
			},
		},
	} {
		t.Run(xtest.CurrentFileLine(), func(t *testing.T) {
			nodes, added, dropped := endpointsDiff(tt.newestEndpoints, tt.previousConns)
			require.Equal(t, tt.nodes, nodes)
			require.Equal(t, tt.added, added)
			require.Equal(t, tt.dropped, dropped)
		})
	}
}

var _ connPool = poolFunc(nil)

type poolFunc func(e endpoint.Info) conn.Conn

func (f poolFunc) Get(e endpoint.Info) conn.Conn {
	return f(e)
}

var _ conn.Conn = (*connMock)(nil)

type connMock struct {
	mock.Conn
}

func (c connMock) Close(ctx context.Context) error {
	panic("implement me")
}

func (c connMock) Invoke(context.Context, string, any, any, ...grpc.CallOption) error {
	panic("implement me")
}

func (c connMock) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	panic("implement me")
}

func TestBalancerWrapCall(t *testing.T) {
	ctx := xtest.Context(t)
	b, err := newBalancer(&balancerConfig.Config{}, poolFunc(func(e endpoint.Info) conn.Conn {
		return &connMock{mock.Conn{EndpointField: e}}
	}), func(b *Balancer) error {
		b.applyDiscoveredEndpoints(ctx, []endpoint.Info{
			&mock.Endpoint{AddressField: "1", LocationField: "a", NodeIDField: 1},
			&mock.Endpoint{AddressField: "2", LocationField: "b", NodeIDField: 2},
			&mock.Endpoint{AddressField: "3", LocationField: "c", NodeIDField: 3},
		}, "")

		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []conn.Conn{
		&connMock{mock.Conn{
			EndpointField: &mock.Endpoint{AddressField: "1", LocationField: "a", NodeIDField: 1},
		}},
		&connMock{mock.Conn{
			EndpointField: &mock.Endpoint{AddressField: "2", LocationField: "b", NodeIDField: 2},
		}},
		&connMock{mock.Conn{
			EndpointField: &mock.Endpoint{AddressField: "3", LocationField: "c", NodeIDField: 3},
		}},
	}, b.connections.Load().prefer)
	for i := range make([]struct{}, 3) {
		err = b.wrapCall(ctx, func(ctx context.Context, cc conn.Conn) error {
			return xerrors.Transport(grpcStatus.Error(grpcCodes.Unavailable, ""))
		})
		require.Error(t, err)
		require.True(t, xerrors.IsTransportError(err, grpcCodes.Unavailable))
		require.Len(t, b.connections.Load().prefer, 2-i)
		require.Len(t, b.connections.Load().fallback, i+1)
	}
	require.Empty(t, b.connections.Load().prefer)
	err = b.wrapCall(ctx, func(ctx context.Context, cc conn.Conn) error {
		return xerrors.Transport(grpcStatus.Error(grpcCodes.Unavailable, ""))
	})
	require.Error(t, err)
	require.True(t, xerrors.IsTransportError(err, grpcCodes.Unavailable))
}

func TestEndpointsToConnections(t *testing.T) {
	p := poolFunc(func(e endpoint.Info) conn.Conn {
		return &connMock{
			mock.Conn{
				EndpointField: &mock.Endpoint{
					AddressField:  e.Address(),
					LocationField: e.Location(),
					NodeIDField:   e.NodeID(),
				},
				StateField: connectivity.Ready,
			},
		}
	})
	endpoints := []endpoint.Info{
		&mock.Endpoint{
			AddressField:  "1",
			LocationField: "a",
			NodeIDField:   1,
		},
		&mock.Endpoint{
			AddressField:  "2",
			LocationField: "b",
			NodeIDField:   2,
		},
		&mock.Endpoint{
			AddressField:  "3",
			LocationField: "c",
			NodeIDField:   3,
		},
	}
	conns := endpointsToConnections(p, endpoints)
	require.Len(t, conns, len(endpoints))
	require.True(t, endpoint.Equals(conns[0], endpoints[0]))
}
