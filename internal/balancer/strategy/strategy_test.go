package strategy

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
)

func TestIdentityEstimators(t *testing.T) {
	endpoints := strategyEndpoints("local", "remote")
	expected := []Estimation{
		{Key: endpoints[0].Key()},
		{Key: endpoints[1].Key()},
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
