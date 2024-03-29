package balancers

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/connectivity"

	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
)

func TestPreferLocalDC(t *testing.T) {
	conns := []conn.Info{
		&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "1", LocationField: "1"}},
		&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "2", LocationField: "2"}, StateField: connectivity.Ready},
		&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "3", LocationField: "2"}, StateField: connectivity.Ready},
	}
	rr := PreferLocalDC(RandomChoice())
	require.False(t, rr.AllowFallback())
	require.Equal(t, []conn.Info{conns[1], conns[2]}, applyPreferFilter(balancerConfig.Info{SelfLocation: "2"}, rr, conns))
}

func TestPreferLocalDCWithFallBack(t *testing.T) {
	conns := []conn.Info{
		&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "1", LocationField: "1"}},
		&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "2", LocationField: "2"}, StateField: connectivity.Ready},
		&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "3", LocationField: "2"}, StateField: connectivity.Ready},
	}
	rr := PreferLocalDCWithFallBack(RandomChoice())
	require.True(t, rr.AllowFallback())
	require.Equal(t, []conn.Info{conns[1], conns[2]}, applyPreferFilter(balancerConfig.Info{SelfLocation: "2"}, rr, conns))
}

func TestPreferLocations(t *testing.T) {
	conns := []conn.Info{
		&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "1", LocationField: "zero"}, StateField: connectivity.Ready},
		&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "2", LocationField: "one"}, StateField: connectivity.Ready},
		&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "3", LocationField: "two"}, StateField: connectivity.Ready},
	}

	rr := PreferLocations(RandomChoice(), "zero", "two")
	require.False(t, rr.AllowFallback())
	require.Equal(t, []conn.Info{conns[0], conns[2]}, applyPreferFilter(balancerConfig.Info{}, rr, conns))
}

func TestPreferLocationsWithFallback(t *testing.T) {
	conns := []conn.Info{
		&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "1", LocationField: "zero"}, StateField: connectivity.Ready},
		&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "2", LocationField: "one"}, StateField: connectivity.Ready},
		&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "3", LocationField: "two"}, StateField: connectivity.Ready},
	}

	rr := PreferLocationsWithFallback(RandomChoice(), "zero", "two")
	require.True(t, rr.AllowFallback())
	require.Equal(t, []conn.Info{conns[0], conns[2]}, applyPreferFilter(balancerConfig.Info{}, rr, conns))
}

func applyPreferFilter(info balancerConfig.Info, b *balancerConfig.Config, conns []conn.Info) []conn.Info {
	res := make([]conn.Info, 0, len(conns))
	for _, c := range conns {
		if b.Filter().Allow(info, c) {
			res = append(res, c)
		}
	}

	return res
}
