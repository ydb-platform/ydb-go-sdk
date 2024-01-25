package balancer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestEndpointsDiff(t *testing.T) {
	for _, tt := range []struct {
		newestEndpoints []endpoint.Endpoint
		previousConns   []conn.Conn
		nodes           []trace.EndpointInfo
		added           []trace.EndpointInfo
		dropped         []trace.EndpointInfo
	}{
		{
			newestEndpoints: []endpoint.Endpoint{
				&mock.Endpoint{AddrField: "1"},
				&mock.Endpoint{AddrField: "3"},
				&mock.Endpoint{AddrField: "2"},
				&mock.Endpoint{AddrField: "0"},
			},
			previousConns: []conn.Conn{
				&mock.Conn{AddrField: "2"},
				&mock.Conn{AddrField: "1"},
				&mock.Conn{AddrField: "0"},
				&mock.Conn{AddrField: "3"},
			},
			nodes: []trace.EndpointInfo{
				&mock.Endpoint{AddrField: "0"},
				&mock.Endpoint{AddrField: "1"},
				&mock.Endpoint{AddrField: "2"},
				&mock.Endpoint{AddrField: "3"},
			},
			added:   []trace.EndpointInfo{},
			dropped: []trace.EndpointInfo{},
		},
		{
			newestEndpoints: []endpoint.Endpoint{
				&mock.Endpoint{AddrField: "1"},
				&mock.Endpoint{AddrField: "3"},
				&mock.Endpoint{AddrField: "2"},
				&mock.Endpoint{AddrField: "0"},
			},
			previousConns: []conn.Conn{
				&mock.Conn{AddrField: "1"},
				&mock.Conn{AddrField: "0"},
				&mock.Conn{AddrField: "3"},
			},
			nodes: []trace.EndpointInfo{
				&mock.Endpoint{AddrField: "0"},
				&mock.Endpoint{AddrField: "1"},
				&mock.Endpoint{AddrField: "2"},
				&mock.Endpoint{AddrField: "3"},
			},
			added: []trace.EndpointInfo{
				&mock.Endpoint{AddrField: "2"},
			},
			dropped: []trace.EndpointInfo{},
		},
		{
			newestEndpoints: []endpoint.Endpoint{
				&mock.Endpoint{AddrField: "1"},
				&mock.Endpoint{AddrField: "3"},
				&mock.Endpoint{AddrField: "0"},
			},
			previousConns: []conn.Conn{
				&mock.Conn{AddrField: "1"},
				&mock.Conn{AddrField: "2"},
				&mock.Conn{AddrField: "0"},
				&mock.Conn{AddrField: "3"},
			},
			nodes: []trace.EndpointInfo{
				&mock.Endpoint{AddrField: "0"},
				&mock.Endpoint{AddrField: "1"},
				&mock.Endpoint{AddrField: "3"},
			},
			added: []trace.EndpointInfo{},
			dropped: []trace.EndpointInfo{
				&mock.Endpoint{AddrField: "2"},
			},
		},
		{
			newestEndpoints: []endpoint.Endpoint{
				&mock.Endpoint{AddrField: "1"},
				&mock.Endpoint{AddrField: "3"},
				&mock.Endpoint{AddrField: "0"},
			},
			previousConns: []conn.Conn{
				&mock.Conn{AddrField: "4"},
				&mock.Conn{AddrField: "7"},
				&mock.Conn{AddrField: "8"},
			},
			nodes: []trace.EndpointInfo{
				&mock.Endpoint{AddrField: "0"},
				&mock.Endpoint{AddrField: "1"},
				&mock.Endpoint{AddrField: "3"},
			},
			added: []trace.EndpointInfo{
				&mock.Endpoint{AddrField: "0"},
				&mock.Endpoint{AddrField: "1"},
				&mock.Endpoint{AddrField: "3"},
			},
			dropped: []trace.EndpointInfo{
				&mock.Endpoint{AddrField: "4"},
				&mock.Endpoint{AddrField: "7"},
				&mock.Endpoint{AddrField: "8"},
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
