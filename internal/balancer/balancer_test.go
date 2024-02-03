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

func TestEndpointsDiff_FirstCase(t *testing.T) {
	newestEndpoints := []endpoint.Endpoint{
		&mock.Endpoint{AddrField: "1"},
		&mock.Endpoint{AddrField: "3"},
		&mock.Endpoint{AddrField: "2"},
		&mock.Endpoint{AddrField: "0"},
	}
	previousConns := []conn.Conn{
		&mock.Conn{AddrField: "2"},
		&mock.Conn{AddrField: "1"},
		&mock.Conn{AddrField: "0"},
		&mock.Conn{AddrField: "3"},
	}
	expectedNodes := []trace.EndpointInfo{
		&mock.Endpoint{AddrField: "0"},
		&mock.Endpoint{AddrField: "1"},
		&mock.Endpoint{AddrField: "2"},
		&mock.Endpoint{AddrField: "3"},
	}
	expectedAdded := []trace.EndpointInfo{}
	expectedDropped := []trace.EndpointInfo{}

	t.Run(xtest.CurrentFileLine(), func(t *testing.T) {
		testEndpointsDiffHelper(t, newestEndpoints, previousConns, expectedNodes, expectedAdded, expectedDropped)
	})
}

func TestEndpointsDiff_SecondCase(t *testing.T) {
	newestEndpoints := []endpoint.Endpoint{
		&mock.Endpoint{AddrField: "1"},
		&mock.Endpoint{AddrField: "3"},
		&mock.Endpoint{AddrField: "2"},
		&mock.Endpoint{AddrField: "0"},
	}
	previousConns := []conn.Conn{
		&mock.Conn{AddrField: "1"},
		&mock.Conn{AddrField: "0"},
		&mock.Conn{AddrField: "3"},
	}
	expectedNodes := []trace.EndpointInfo{
		&mock.Endpoint{AddrField: "0"},
		&mock.Endpoint{AddrField: "1"},
		&mock.Endpoint{AddrField: "2"},
		&mock.Endpoint{AddrField: "3"},
	}
	expectedAdded := []trace.EndpointInfo{
		&mock.Endpoint{AddrField: "2"},
	}
	expectedDropped := []trace.EndpointInfo{}

	t.Run(xtest.CurrentFileLine(), func(t *testing.T) {
		testEndpointsDiffHelper(t, newestEndpoints, previousConns, expectedNodes, expectedAdded, expectedDropped)
	})
}

func TestEndpointsDiff_ThirdCase(t *testing.T) {
	newestEndpoints := []endpoint.Endpoint{
		&mock.Endpoint{AddrField: "1"},
		&mock.Endpoint{AddrField: "3"},
		&mock.Endpoint{AddrField: "0"},
	}
	previousConns := []conn.Conn{
		&mock.Conn{AddrField: "1"},
		&mock.Conn{AddrField: "2"},
		&mock.Conn{AddrField: "0"},
		&mock.Conn{AddrField: "3"},
	}
	expectedNodes := []trace.EndpointInfo{
		&mock.Endpoint{AddrField: "0"},
		&mock.Endpoint{AddrField: "1"},
		&mock.Endpoint{AddrField: "3"},
	}
	expectedAdded := []trace.EndpointInfo{}
	expectedDropped := []trace.EndpointInfo{
		&mock.Endpoint{AddrField: "2"},
	}

	t.Run(xtest.CurrentFileLine(), func(t *testing.T) {
		testEndpointsDiffHelper(t, newestEndpoints, previousConns, expectedNodes, expectedAdded, expectedDropped)
	})
}

func TestEndpointsDiff_FourthCase(t *testing.T) {
	newestEndpoints := []endpoint.Endpoint{
		&mock.Endpoint{AddrField: "1"},
		&mock.Endpoint{AddrField: "3"},
		&mock.Endpoint{AddrField: "0"},
	}
	previousConns := []conn.Conn{
		&mock.Conn{AddrField: "4"},
		&mock.Conn{AddrField: "7"},
		&mock.Conn{AddrField: "8"},
	}
	expectedNodes := []trace.EndpointInfo{
		&mock.Endpoint{AddrField: "0"},
		&mock.Endpoint{AddrField: "1"},
		&mock.Endpoint{AddrField: "3"},
	}
	expectedAdded := []trace.EndpointInfo{
		&mock.Endpoint{AddrField: "0"},
		&mock.Endpoint{AddrField: "1"},
		&mock.Endpoint{AddrField: "3"},
	}
	expectedDropped := []trace.EndpointInfo{
		&mock.Endpoint{AddrField: "4"},
		&mock.Endpoint{AddrField: "7"},
		&mock.Endpoint{AddrField: "8"},
	}

	t.Run(xtest.CurrentFileLine(), func(t *testing.T) {
		testEndpointsDiffHelper(t, newestEndpoints, previousConns, expectedNodes, expectedAdded, expectedDropped)
	})
}

func testEndpointsDiffHelper(t *testing.T, newestEndpoints []endpoint.Endpoint, previousConns []conn.Conn, expectedNodes, expectedAdded, expectedDropped []trace.EndpointInfo) {
	nodes, added, dropped := endpointsDiff(newestEndpoints, previousConns)
	require.Equal(t, expectedNodes, nodes)
	require.Equal(t, expectedAdded, added)
	require.Equal(t, expectedDropped, dropped)
}
