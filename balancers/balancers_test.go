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
	require.Len(t, rr.Estimate(strategy.Info{SelfLocation: "2"}, connEndpoints(conns)), 2)
	require.Equal(t, []conn.Conn{conns[1], conns[2]}, applyPreferEstimator(strategy.Info{SelfLocation: "2"}, rr, conns))
}

func TestPreferLocalDCWithFallBack(t *testing.T) {
	conns := []conn.Conn{
		&mock.Conn{AddrField: "1", LocationField: "1"},
		&mock.Conn{AddrField: "2", StateField: state.Online, LocationField: "2"},
		&mock.Conn{AddrField: "3", StateField: state.Online, LocationField: "2"},
	}
	rr := PreferNearestDCWithFallBack(RandomChoice())
	require.Equal(t, 2, estimationGroupCount(rr.Estimate(strategy.Info{SelfLocation: "2"}, connEndpoints(conns))))
	require.Equal(t, []conn.Conn{conns[1], conns[2]}, applyPreferEstimator(strategy.Info{SelfLocation: "2"}, rr, conns))
}

func TestPreferLocations(t *testing.T) {
	conns := []conn.Conn{
		&mock.Conn{AddrField: "1", LocationField: "zero", StateField: state.Online},
		&mock.Conn{AddrField: "2", StateField: state.Online, LocationField: "one"},
		&mock.Conn{AddrField: "3", StateField: state.Online, LocationField: "two"},
	}

	rr := PreferLocations(RandomChoice(), "zero", "two")
	require.Len(t, rr.Estimate(strategy.Info{}, connEndpoints(conns)), 2)
	require.Equal(t, []conn.Conn{conns[0], conns[2]}, applyPreferEstimator(strategy.Info{}, rr, conns))
}

func TestPreferLocationsWithFallback(t *testing.T) {
	conns := []conn.Conn{
		&mock.Conn{AddrField: "1", LocationField: "zero", StateField: state.Online},
		&mock.Conn{AddrField: "2", StateField: state.Online, LocationField: "one"},
		&mock.Conn{AddrField: "3", StateField: state.Online, LocationField: "two"},
	}

	rr := PreferLocationsWithFallback(RandomChoice(), "zero", "two")
	require.Equal(t, 2, estimationGroupCount(rr.Estimate(strategy.Info{}, connEndpoints(conns))))
	require.Equal(t, []conn.Conn{conns[0], conns[2]}, applyPreferEstimator(strategy.Info{}, rr, conns))
}

func TestDeprecatedLocalDCAliases(t *testing.T) {
	conns := []conn.Conn{
		&mock.Conn{AddrField: "1", LocationField: "local", StateField: state.Online},
		&mock.Conn{AddrField: "2", LocationField: "remote", StateField: state.Online},
	}
	info := strategy.Info{SelfLocation: "local"}

	withoutFallback := PreferLocalDC(RandomChoice())
	require.Equal(t, []conn.Conn{conns[0]}, applyPreferEstimator(info, withoutFallback, conns))

	withFallback := PreferLocalDCWithFallBack(RandomChoice())
	require.Equal(t, 2, estimationGroupCount(withFallback.Estimate(info, connEndpoints(conns))))
}

func TestPreferLocationsRejectsEmptyList(t *testing.T) {
	require.Panics(t, func() {
		PreferLocations(RandomChoice())
	})
	require.Panics(t, func() {
		PreferLocationsWithFallback(RandomChoice())
	})
}

func TestCustomPrefer(t *testing.T) {
	conns := []conn.Conn{
		&mock.Conn{AddrField: "1", NodeIDField: 1, StateField: state.Online},
		&mock.Conn{AddrField: "2", NodeIDField: 2, StateField: state.Online},
	}
	filter := func(candidate Endpoint) bool {
		return candidate.NodeID()%2 == 0
	}

	withoutFallback := Prefer(RandomChoice(), filter)
	require.Equal(t, []conn.Conn{conns[1]}, applyPreferEstimator(strategy.Info{}, withoutFallback, conns))
	require.Contains(t, withoutFallback.String(), "Filter=Custom")

	withFallback := PreferWithFallback(RandomChoice(), filter)
	require.Equal(t, 2, estimationGroupCount(withFallback.Estimate(strategy.Info{}, connEndpoints(conns))))
}

func applyPreferEstimator(info strategy.Info, estimator strategy.Estimator, conns []conn.Conn) []conn.Conn {
	estimates := estimator.Estimate(info, connEndpoints(conns))
	if len(estimates) == 0 {
		return nil
	}

	minimum := estimates[0].Penalty
	for _, estimation := range estimates {
		minimum = min(minimum, estimation.Penalty)
	}
	allowed := make(map[endpoint.Key]struct{}, len(estimates))
	for _, estimation := range estimates {
		if estimation.Penalty == minimum {
			allowed[estimation.Key] = struct{}{}
		}
	}

	res := make([]conn.Conn, 0, len(allowed))
	for _, c := range conns {
		if _, ok := allowed[c.Endpoint().Key()]; ok {
			res = append(res, c)
		}
	}

	return res
}

func estimationGroupCount(estimates []strategy.Estimation) int {
	penalties := make(map[uint64]struct{})
	for _, estimation := range estimates {
		penalties[estimation.Penalty] = struct{}{}
	}

	return len(penalties)
}

func connEndpoints(conns []conn.Conn) []endpoint.Endpoint {
	result := make([]endpoint.Endpoint, 0, len(conns))
	for _, connection := range conns {
		result = append(result, connection.Endpoint())
	}

	return result
}
