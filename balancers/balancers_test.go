package balancers

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
	routerconfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/router/config"
)

func TestPreferLocalDC(t *testing.T) {
	conns := []conn.Conn{
		&mock.Conn{AddrField: "1", LocationField: "1"},
		&mock.Conn{AddrField: "2", State: conn.Online, LocationField: "2"},
		&mock.Conn{AddrField: "3", State: conn.Online, LocationField: "2"},
	}
	rr := PreferLocalDC(Default())
	require.False(t, rr.AllowFalback)
	require.Equal(t, []conn.Conn{conns[1], conns[2]}, applyPreferFilter(routerconfig.Info{SelfLocation: "2"}, rr, conns))
}

func TestPreferLocalDCWithFallBack(t *testing.T) {
	conns := []conn.Conn{
		&mock.Conn{AddrField: "1", LocationField: "1"},
		&mock.Conn{AddrField: "2", State: conn.Online, LocationField: "2"},
		&mock.Conn{AddrField: "3", State: conn.Online, LocationField: "2"},
	}
	rr := PreferLocalDCWithFallBack(Default())
	require.True(t, rr.AllowFalback)
	require.Equal(t, []conn.Conn{conns[1], conns[2]}, applyPreferFilter(routerconfig.Info{SelfLocation: "2"}, rr, conns))
}

func TestPreferLocations(t *testing.T) {
	conns := []conn.Conn{
		&mock.Conn{AddrField: "1", LocationField: "zero", State: conn.Online},
		&mock.Conn{AddrField: "2", State: conn.Online, LocationField: "one"},
		&mock.Conn{AddrField: "3", State: conn.Online, LocationField: "two"},
	}

	rr := PreferLocations(Default(), "zero", "two")
	require.False(t, rr.AllowFalback)
	require.Equal(t, []conn.Conn{conns[0], conns[2]}, applyPreferFilter(routerconfig.Info{}, rr, conns))
}

func TestPreferLocationsWithFallback(t *testing.T) {
	conns := []conn.Conn{
		&mock.Conn{AddrField: "1", LocationField: "zero", State: conn.Online},
		&mock.Conn{AddrField: "2", State: conn.Online, LocationField: "one"},
		&mock.Conn{AddrField: "3", State: conn.Online, LocationField: "two"},
	}

	rr := PreferLocationsWithFallback(Default(), "zero", "two")
	require.True(t, rr.AllowFalback)
	require.Equal(t, []conn.Conn{conns[0], conns[2]}, applyPreferFilter(routerconfig.Info{}, rr, conns))
}

func applyPreferFilter(info routerconfig.Info, b *routerconfig.Config, conns []conn.Conn) []conn.Conn {
	if b.IsPreferConn == nil {
		b.IsPreferConn = func(routerInfo routerconfig.Info, c conn.Conn) bool { return true }
	}
	res := make([]conn.Conn, 0, len(conns))
	for _, c := range conns {
		if b.IsPreferConn(info, c) {
			res = append(res, c)
		}
	}
	return res
}
