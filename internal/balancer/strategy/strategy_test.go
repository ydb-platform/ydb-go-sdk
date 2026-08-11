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

func TestEstimatorDiscoveryRequirements(t *testing.T) {
	tests := []struct {
		name                       string
		estimator                  Estimator
		expectedConfiguredEndpoint bool
		expectedNearestDC          bool
	}{
		{name: "nil defaults", estimator: nil},
		{name: "unknown estimator", estimator: fixedEstimator{}},
		{
			name:                       "preference preserves single connection source",
			estimator:                  Prefer(SingleConn(), "local", locationMatch("local"), false),
			expectedConfiguredEndpoint: true,
		},
		{
			name: "nearest DC over dynamic discovery",
			estimator: PreferNearestDC(
				RandomChoice(), "local", locationMatch("local"), true,
			),
			expectedNearestDC: true,
		},
		{
			name: "outer preference preserves nested requirements",
			estimator: Prefer(
				PreferNearestDC(SingleConn(), "local", locationMatch("local"), true),
				"remote", locationMatch("remote"), false,
			),
			expectedConfiguredEndpoint: true,
			expectedNearestDC:          true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expectedConfiguredEndpoint, UsesConfiguredEndpoint(test.estimator))
			require.Equal(t, test.expectedNearestDC, DetectsNearestDC(test.estimator))
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
		{Key: endpoints[0].Key(), Weight: 3},
		{Key: endpoints[1].Key(), Penalty: 2, Weight: 4},
	}, estimator.Estimate(Info{}, endpoints))

	overflowSafe := Prefer(fixedEstimator{estimates: []Estimation{
		{Key: endpoints[0].Key(), Penalty: math.MaxUint64, Weight: 1},
		{Key: endpoints[1].Key(), Weight: 1},
	}}, "local", locationMatch("local"), true)
	require.Equal(t, []Estimation{
		{Key: endpoints[0].Key(), Penalty: 1, Weight: 1},
		{Key: endpoints[1].Key(), Penalty: 2, Weight: 1},
	}, overflowSafe.Estimate(Info{}, endpoints))
}

func TestPreferCallsChildOnceWithFullSnapshot(t *testing.T) {
	endpoints := strategyEndpoints("local", "remote")
	child := &recordingEstimator{}
	estimator := Prefer(child, "local", locationMatch("local"), true)

	estimator.Estimate(Info{}, endpoints)

	require.Equal(t, 1, child.calls)
	require.Equal(t, endpoints, child.endpoints)
}

func TestPartitionEstimates(t *testing.T) {
	endpoints := strategyEndpoints("local", "remote")
	estimates := RandomChoice().Estimate(Info{}, endpoints)

	preferred, fallback := partitionEstimates(endpoints, estimates, nil, Info{})
	require.Equal(t, estimates, preferred)
	require.Nil(t, fallback)

	preferred, fallback = partitionEstimates(endpoints, estimates, locationMatch("local"), Info{})
	require.Equal(t, estimates[:1], preferred)
	require.Equal(t, estimates[1:], fallback)
	require.Nil(t, compactPenalties(nil))
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

type recordingEstimator struct {
	calls     int
	endpoints []endpoint.Endpoint
}

func (r *recordingEstimator) Estimate(_ Info, endpoints []endpoint.Endpoint) []Estimation {
	r.calls++
	r.endpoints = endpoints

	return randomChoice{}.Estimate(Info{}, endpoints)
}

func (*recordingEstimator) String() string {
	return "Recording"
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
