package balancers

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/routerconfig"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

func TestPreferLocalDC(t *testing.T) {
	conns := []conn.Conn{
		&mock.Conn{AddrField: "1", LocalDCField: false},
		&mock.Conn{AddrField: "2", State: conn.Online, LocalDCField: true},
		&mock.Conn{AddrField: "3", State: conn.Online, LocalDCField: true},
	}
	rr := PreferLocalDC(RoundRobin())
	require.False(t, rr.AllowFalback)
	require.Equal(t, []conn.Conn{conns[1], conns[2]}, applyPreferFilter(rr, conns))
}

func TestPreferLocalDCWithFallBack(t *testing.T) {
	conns := []conn.Conn{
		&mock.Conn{AddrField: "1", LocalDCField: false},
		&mock.Conn{AddrField: "2", State: conn.Online, LocalDCField: true},
		&mock.Conn{AddrField: "3", State: conn.Online, LocalDCField: true},
	}
	rr := PreferLocalDCWithFallBack(RoundRobin())
	require.True(t, rr.AllowFalback)
	require.Equal(t, []conn.Conn{conns[1], conns[2]}, applyPreferFilter(rr, conns))
}

func TestPreferLocations(t *testing.T) {
	conns := []conn.Conn{
		&mock.Conn{AddrField: "1", LocationField: "zero", State: conn.Online},
		&mock.Conn{AddrField: "2", State: conn.Online, LocationField: "one"},
		&mock.Conn{AddrField: "3", State: conn.Online, LocationField: "two"},
	}

	rr := PreferLocations(RoundRobin(), "zero", "two")
	require.False(t, rr.AllowFalback)
	require.Equal(t, []conn.Conn{conns[0], conns[2]}, applyPreferFilter(rr, conns))
}

func TestPreferLocationsWithFallback(t *testing.T) {
	conns := []conn.Conn{
		&mock.Conn{AddrField: "1", LocationField: "zero", State: conn.Online},
		&mock.Conn{AddrField: "2", State: conn.Online, LocationField: "one"},
		&mock.Conn{AddrField: "3", State: conn.Online, LocationField: "two"},
	}

	rr := PreferLocationsWithFallback(RoundRobin(), "zero", "two")
	require.True(t, rr.AllowFalback)
	require.Equal(t, []conn.Conn{conns[0], conns[2]}, applyPreferFilter(rr, conns))
}

func applyPreferFilter(b *routerconfig.Config, conns []conn.Conn) []conn.Conn {
	if b.IsPreferConn == nil {
		b.IsPreferConn = func(c conn.Conn) bool { return true }
	}
	res := make([]conn.Conn, 0, len(conns))
	for _, c := range conns {
		if b.IsPreferConn(c) {
			res = append(res, c)
		}
	}
	return res
}
