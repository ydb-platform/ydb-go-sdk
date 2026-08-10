package strategy

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
)

func TestIdentityEstimators(t *testing.T) {
	endpoints := strategyEndpoints("local", "remote")
	expected := []Estimation{
		{Key: endpoints[0].Key(), Weight: 1},
		{Key: endpoints[1].Key(), Weight: 1},
	}

	tests := []struct {
		name      string
		estimator Estimator
		expected  string
	}{
		{name: "random choice", estimator: RandomChoice(), expected: "RandomChoice"},
		{name: "single connection", estimator: SingleConn(), expected: "SingleConn"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, test.estimator.String())
			require.Equal(t, expected, test.estimator.Estimate(Info{}, endpoints))
		})
	}
}

func TestPreferEstimator(t *testing.T) {
	endpoints := strategyEndpoints("local", "remote")
	withoutFallback := PreferNearestDC(RandomChoice(), "Location(local)", locationMatch("local"), false)

	require.Equal(t, "Prefer{Filter=Location(local),AllowFallback=false,Child=RandomChoice}",
		withoutFallback.String(),
	)
	require.Equal(t, []Estimation{{Key: endpoints[0].Key(), Weight: 1}},
		withoutFallback.Estimate(Info{}, endpoints),
	)

	withFallback := Prefer(SingleConn(), "Location(local)", locationMatch("local"), true)
	require.Equal(t, []Estimation{
		{Key: endpoints[0].Key(), Weight: 1},
		{Key: endpoints[1].Key(), Penalty: 1, Weight: 1},
	}, withFallback.Estimate(Info{}, endpoints))

	noPreferred := Prefer(RandomChoice(), "missing", locationMatch("missing"), true)
	require.Equal(t, []Estimation{
		{Key: endpoints[0].Key(), Weight: 1},
		{Key: endpoints[1].Key(), Weight: 1},
	}, noPreferred.Estimate(Info{}, endpoints))

	normalized := Prefer(nil, "Location(local)", locationMatch("local"), false)
	require.Equal(t, "Prefer{Filter=Location(local),AllowFallback=false,Child=RandomChoice}", normalized.String())
}

func TestPreferPenaltyComposition(t *testing.T) {
	endpoints := strategyEndpoints("local", "remote", "other")
	child := fixedEstimator{estimates: []Estimation{
		{Key: endpoints[0].Key(), Penalty: 2, Weight: 3},
		{Key: endpoints[1].Key(), Penalty: 5, Weight: 4},
	}}
	estimator := Prefer(child, "local", locationMatch("local"), true)

	require.Equal(t, []Estimation{
		{Key: endpoints[0].Key(), Penalty: 2, Weight: 3},
		{Key: endpoints[1].Key(), Penalty: 8, Weight: 4},
	}, estimator.Estimate(Info{}, endpoints))

	require.Equal(t, uint64(math.MaxUint64), addPenalty(math.MaxUint64, 1))
	require.Equal(t, uint64(math.MaxUint64), fallbackPenaltyShift([]Estimation{{Penalty: math.MaxUint64}}))
}

func TestPartitionEndpoints(t *testing.T) {
	endpoints := strategyEndpoints("local", "remote")

	preferred, fallback := partitionEndpoints(endpoints, nil, Info{})
	require.Equal(t, endpoints, preferred)
	require.Nil(t, fallback)

	preferred, fallback = partitionEndpoints(endpoints, locationMatch("local"), Info{})
	require.Equal(t, endpoints[:1], preferred)
	require.Equal(t, endpoints[1:], fallback)
}

func TestEstimatorCanUseEndpointMetadata(t *testing.T) {
	endpoints := []endpoint.Endpoint{
		endpoint.New("primary", endpoint.WithMetadata(endpoint.Metadata{
			BridgePileState: endpoint.PileStatePrimary,
		})),
		endpoint.New("secondary", endpoint.WithMetadata(endpoint.Metadata{
			BridgePileState: endpoint.PileStateSynchronized,
		})),
	}
	estimator := Prefer(RandomChoice(), "PrimaryPile", func(_ Info, candidate endpoint.Info) bool {
		return candidate.Metadata().BridgePileState == endpoint.PileStatePrimary
	}, false)

	require.Equal(t, []Estimation{{Key: endpoints[0].Key(), Weight: 1}}, estimator.Estimate(Info{}, endpoints))
}

func strategyEndpoints(locations ...string) []endpoint.Endpoint {
	result := make([]endpoint.Endpoint, len(locations))
	for i, location := range locations {
		result[i] = endpoint.New(location, endpoint.WithID(uint32(i+1)), endpoint.WithLocation(location))
	}

	return result
}

func locationMatch(location string) func(Info, endpoint.Info) bool {
	return func(_ Info, candidate endpoint.Info) bool {
		return candidate.Location() == location
	}
}

type fixedEstimator struct {
	estimates []Estimation
}

func (f fixedEstimator) Estimate(_ Info, endpoints []endpoint.Endpoint) []Estimation {
	keys := make(map[endpoint.Key]struct{}, len(endpoints))
	for _, candidate := range endpoints {
		keys[candidate.Key()] = struct{}{}
	}
	result := make([]Estimation, 0, len(f.estimates))
	for _, estimation := range f.estimates {
		if _, ok := keys[estimation.Key]; ok {
			result = append(result, estimation)
		}
	}

	return result
}

func (fixedEstimator) String() string {
	return "Fixed"
}

type testRand struct {
	index int
}

func (testRand) Int64(int64) int64 {
	return 0
}

func (r testRand) Int(maximum int) int {
	return r.index % maximum
}

func (testRand) Shuffle(n int, swap func(i, j int)) {
	if n > 1 {
		swap(0, n-1)
	}
}
