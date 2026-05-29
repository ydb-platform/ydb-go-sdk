package balancers

import (
	"testing"

	"github.com/stretchr/testify/require"

	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
)

func TestPreferLocalDC(t *testing.T) {
	conns := []conn.Conn{
		&mock.Conn{AddrField: "1", LocationField: "1"},
		&mock.Conn{AddrField: "2", State: conn.Online, LocationField: "2"},
		&mock.Conn{AddrField: "3", State: conn.Online, LocationField: "2"},
	}
	rr := PreferNearestDC(RandomChoice())
	require.False(t, rr.AllowFallback)
	require.Equal(t, []conn.Conn{conns[1], conns[2]}, applyPreferFilter(balancerConfig.Info{SelfLocation: "2"}, rr, conns))
}

func TestPreferLocalDCWithFallBack(t *testing.T) {
	conns := []conn.Conn{
		&mock.Conn{AddrField: "1", LocationField: "1"},
		&mock.Conn{AddrField: "2", State: conn.Online, LocationField: "2"},
		&mock.Conn{AddrField: "3", State: conn.Online, LocationField: "2"},
	}
	rr := PreferNearestDCWithFallBack(RandomChoice())
	require.True(t, rr.AllowFallback)
	require.Equal(t, []conn.Conn{conns[1], conns[2]}, applyPreferFilter(balancerConfig.Info{SelfLocation: "2"}, rr, conns))
}

func TestPreferLocations(t *testing.T) {
	conns := []conn.Conn{
		&mock.Conn{AddrField: "1", LocationField: "zero", State: conn.Online},
		&mock.Conn{AddrField: "2", State: conn.Online, LocationField: "one"},
		&mock.Conn{AddrField: "3", State: conn.Online, LocationField: "two"},
	}

	rr := PreferLocations(RandomChoice(), "zero", "two")
	require.False(t, rr.AllowFallback)
	require.Equal(t, []conn.Conn{conns[0], conns[2]}, applyPreferFilter(balancerConfig.Info{}, rr, conns))
}

func TestPreferLocationsWithFallback(t *testing.T) {
	conns := []conn.Conn{
		&mock.Conn{AddrField: "1", LocationField: "zero", State: conn.Online},
		&mock.Conn{AddrField: "2", State: conn.Online, LocationField: "one"},
		&mock.Conn{AddrField: "3", State: conn.Online, LocationField: "two"},
	}

	rr := PreferLocationsWithFallback(RandomChoice(), "zero", "two")
	require.True(t, rr.AllowFallback)
	require.Equal(t, []conn.Conn{conns[0], conns[2]}, applyPreferFilter(balancerConfig.Info{}, rr, conns))
}

func applyPreferFilter(info balancerConfig.Info, b *balancerConfig.Config, conns []conn.Conn) []conn.Conn {
	if b.Filter == nil {
		b.Filter = filterFunc(func(info balancerConfig.Info, e endpoint.Info) bool { return true })
	}
	res := make([]conn.Conn, 0, len(conns))
	for _, c := range conns {
		if b.Filter.Allow(info, c.Endpoint()) {
			res = append(res, c)
		}
	}

	return res
}
