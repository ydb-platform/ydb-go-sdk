package balancers

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/strategy"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
)

func TestPreferLocalDC(t *testing.T) {
	conns := []conn.Conn{
		&mock.Conn{AddrField: "1", LocationField: "1"},
		&mock.Conn{AddrField: "2", StateField: state.Online, LocationField: "2"},
		&mock.Conn{AddrField: "3", StateField: state.Online, LocationField: "2"},
	}
	rr := PreferNearestDC(RandomChoice())
	require.True(t, rr.Requirements().DetectNearestDC)
	require.Len(t, rr.Filter(strategy.Info{SelfLocation: "2"}, connEndpoints(conns)), 1)
	require.Equal(t, []conn.Conn{conns[1], conns[2]}, applyPreferFilter(strategy.Info{SelfLocation: "2"}, rr, conns))
}

func TestPreferLocalDCWithFallBack(t *testing.T) {
	conns := []conn.Conn{
		&mock.Conn{AddrField: "1", LocationField: "1"},
		&mock.Conn{AddrField: "2", StateField: state.Online, LocationField: "2"},
		&mock.Conn{AddrField: "3", StateField: state.Online, LocationField: "2"},
	}
	rr := PreferNearestDCWithFallBack(RandomChoice())
	require.True(t, rr.Requirements().DetectNearestDC)
	require.Len(t, rr.Filter(strategy.Info{SelfLocation: "2"}, connEndpoints(conns)), 2)
	require.Equal(t, []conn.Conn{conns[1], conns[2]}, applyPreferFilter(strategy.Info{SelfLocation: "2"}, rr, conns))
}

func TestPreferLocations(t *testing.T) {
	conns := []conn.Conn{
		&mock.Conn{AddrField: "1", LocationField: "zero", StateField: state.Online},
		&mock.Conn{AddrField: "2", StateField: state.Online, LocationField: "one"},
		&mock.Conn{AddrField: "3", StateField: state.Online, LocationField: "two"},
	}

	rr := PreferLocations(RandomChoice(), "zero", "two")
	require.Len(t, rr.Filter(strategy.Info{}, connEndpoints(conns)), 1)
	require.Equal(t, []conn.Conn{conns[0], conns[2]}, applyPreferFilter(strategy.Info{}, rr, conns))
}

func TestPreferLocationsWithFallback(t *testing.T) {
	conns := []conn.Conn{
		&mock.Conn{AddrField: "1", LocationField: "zero", StateField: state.Online},
		&mock.Conn{AddrField: "2", StateField: state.Online, LocationField: "one"},
		&mock.Conn{AddrField: "3", StateField: state.Online, LocationField: "two"},
	}

	rr := PreferLocationsWithFallback(RandomChoice(), "zero", "two")
	require.Len(t, rr.Filter(strategy.Info{}, connEndpoints(conns)), 2)
	require.Equal(t, []conn.Conn{conns[0], conns[2]}, applyPreferFilter(strategy.Info{}, rr, conns))
}

func applyPreferFilter(info strategy.Info, b Balancer, conns []conn.Conn) []conn.Conn {
	groups := b.Filter(info, connEndpoints(conns))
	if len(groups) == 0 {
		return nil
	}

	allowed := make(map[endpoint.Key]struct{}, len(groups[0]))
	for _, candidate := range groups[0] {
		allowed[candidate.Key()] = struct{}{}
	}

	res := make([]conn.Conn, 0, len(groups[0]))
	for _, c := range conns {
		if _, ok := allowed[c.Endpoint().Key()]; ok {
			res = append(res, c)
		}
	}

	return res
}

func connEndpoints(conns []conn.Conn) []endpoint.Endpoint {
	result := make([]endpoint.Endpoint, 0, len(conns))
	for _, connection := range conns {
		result = append(result, connection.Endpoint())
	}

	return result
}
