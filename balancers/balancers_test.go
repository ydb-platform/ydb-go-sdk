package balancers

import (
	"testing"

	"github.com/stretchr/testify/require"

	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
)

func TestPreferLocalDC(t *testing.T) {
	conns := []conn.Conn{
		&mock.Conn{AddrField: "1", LocationField: "1"},
		&mock.Conn{AddrField: "2", State: state.Online, LocationField: "2"},
		&mock.Conn{AddrField: "3", State: state.Online, LocationField: "2"},
	}
	rr := PreferNearestDC(RandomChoice())
	require.False(t, rr.AllowFallback)
	require.Equal(t, []conn.Conn{conns[1], conns[2]}, applyPreferFilter(balancerConfig.Info{SelfLocation: "2"}, rr, conns))
}

func TestPreferLocalDCWithFallBack(t *testing.T) {
	conns := []conn.Conn{
		&mock.Conn{AddrField: "1", LocationField: "1"},
		&mock.Conn{AddrField: "2", State: state.Online, LocationField: "2"},
		&mock.Conn{AddrField: "3", State: state.Online, LocationField: "2"},
	}
	rr := PreferNearestDCWithFallBack(RandomChoice())
	require.True(t, rr.AllowFallback)
	require.Equal(t, []conn.Conn{conns[1], conns[2]}, applyPreferFilter(balancerConfig.Info{SelfLocation: "2"}, rr, conns))
}

func TestPreferLocations(t *testing.T) {
	conns := []conn.Conn{
		&mock.Conn{AddrField: "1", LocationField: "zero", State: state.Online},
		&mock.Conn{AddrField: "2", State: state.Online, LocationField: "one"},
		&mock.Conn{AddrField: "3", State: state.Online, LocationField: "two"},
	}

	rr := PreferLocations(RandomChoice(), "zero", "two")
	require.False(t, rr.AllowFallback)
	require.Equal(t, []conn.Conn{conns[0], conns[2]}, applyPreferFilter(balancerConfig.Info{}, rr, conns))
}

func TestPreferLocationsWithFallback(t *testing.T) {
	conns := []conn.Conn{
		&mock.Conn{AddrField: "1", LocationField: "zero", State: state.Online},
		&mock.Conn{AddrField: "2", State: state.Online, LocationField: "one"},
		&mock.Conn{AddrField: "3", State: state.Online, LocationField: "two"},
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

func TestWithTypeIP(t *testing.T) {
	t.Run("DefaultIsRandomChoice", func(t *testing.T) {
		cfg := WithTypeIP(IPv6)
		require.NotNil(t, cfg)
		require.Equal(t, IPv6, cfg.AllowedIPTypes)
	})

	t.Run("FilterNilWhenNoRestriction", func(t *testing.T) {
		// Zero mask – no filtering.
		var noMask IPType
		require.Nil(t, noMask.Filter())

		// Both families – no filtering needed.
		bothFamilies := IPv4 | IPv6
		require.Nil(t, bothFamilies.Filter())
	})

	t.Run("FilterIPv6Only", func(t *testing.T) {
		filter := IPv6.Filter()
		require.NotNil(t, filter)

		// IPv4 literals must be rejected.
		require.False(t, filter("1.2.3.4:2135"))
		require.False(t, filter("10.0.0.1:2135"))

		// IPv6 literals must be accepted.
		require.True(t, filter("[::1]:2135"))
		require.True(t, filter("[2001:db8::1]:2135"))

		// Malformed / non-IP (should pass through).
		require.True(t, filter("not-an-ip:2135"))
	})

	t.Run("FilterIPv4Only", func(t *testing.T) {
		filter := IPv4.Filter()
		require.NotNil(t, filter)

		// IPv4 literals must be accepted.
		require.True(t, filter("1.2.3.4:2135"))

		// IPv6 literals must be rejected.
		require.False(t, filter("[::1]:2135"))
		require.False(t, filter("[2001:db8::1]:2135"))
	})
}
