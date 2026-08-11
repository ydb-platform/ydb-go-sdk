package balancers

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/strategy"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
)

func TestFromConfig(t *testing.T) {
	tests := []struct {
		name     string
		config   string
		expected string
		fail     bool
	}{
		{name: "empty", config: ``, fail: true},
		{name: "invalid JSON", config: `{`, fail: true},
		{name: "disable", config: `disable`, expected: "SingleConn"},
		{name: "single", config: `single`, expected: "SingleConn"},
		{name: "single/JSON", config: `{"type":"single"}`, expected: "SingleConn"},
		{name: "round_robin", config: `round_robin`, expected: "RandomChoice"},
		{name: "round_robin/JSON", config: `{"type":"round_robin"}`, expected: "RandomChoice"},
		{name: "random_choice", config: `random_choice`, expected: "RandomChoice"},
		{name: "random_choice/JSON", config: `{"type":"random_choice"}`, expected: "RandomChoice"},
		{
			name:     "prefer_local_dc",
			config:   `{"type":"random_choice","prefer":"local_dc"}`,
			expected: "Prefer{Filter=LocalDC,AllowFallback=false,Child=RandomChoice}",
		},
		{
			name:     "prefer_nearest_dc",
			config:   `{"type":"random_choice","prefer":"nearest_dc"}`,
			expected: "Prefer{Filter=LocalDC,AllowFallback=false,Child=RandomChoice}",
		},
		{
			name:   "prefer_unknown_type",
			config: `{"type":"unknown_type","prefer":"local_dc"}`,
			fail:   true,
		},
		{
			name:     "unknown preference",
			config:   `{"type":"random_choice","prefer":"unknown"}`,
			expected: "RandomChoice",
		},
		{
			name:     "prefer_local_dc_with_fallback",
			config:   `{"type":"random_choice","prefer":"local_dc","fallback":true}`,
			expected: "Prefer{Filter=LocalDC,AllowFallback=true,Child=RandomChoice}",
		},
		{
			name:     "prefer_nearest_dc_with_fallback",
			config:   `{"type":"random_choice","prefer":"nearest_dc","fallback":true}`,
			expected: "Prefer{Filter=LocalDC,AllowFallback=true,Child=RandomChoice}",
		},
		{
			name:     "prefer_locations",
			config:   `{"type":"random_choice","prefer":"locations","locations":["AAA","BBB","CCC"]}`,
			expected: "Prefer{Filter=Locations{AAA,BBB,CCC},AllowFallback=false,Child=RandomChoice}",
		},
		{
			name:     "prefer_locations_with_fallback",
			config:   `{"type":"random_choice","prefer":"locations","locations":["AAA","BBB","CCC"],"fallback":true}`,
			expected: "Prefer{Filter=Locations{AAA,BBB,CCC},AllowFallback=true,Child=RandomChoice}",
		},
		{
			name:   "prefer_locations_without_locations",
			config: `{"type":"random_choice","prefer":"locations"}`,
			fail:   true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var actualErr error
			fallback := SingleConn()
			balancer := FromConfig(
				test.config,
				WithParseErrorFallbackBalancer(fallback),
				WithParseErrorHandler(func(err error) {
					actualErr = err
				}),
			)

			if test.fail {
				require.Error(t, actualErr)
				require.Equal(t, fallback, balancer)

				return
			}

			require.NoError(t, actualErr)
			require.Equal(t, test.expected, balancer.String())
		})
	}
}

func TestFromConfigLogicalCompatibility(t *testing.T) {
	tests := []struct {
		name                   string
		serialized             string
		preferred              []uint32
		fallback               []uint32
		nearestDC              bool
		usesConfiguredEndpoint bool
	}{
		{name: "disable", serialized: `disable`, preferred: []uint32{1, 2, 3}, usesConfiguredEndpoint: true},
		{name: "single", serialized: `single`, preferred: []uint32{1, 2, 3}, usesConfiguredEndpoint: true},
		{name: "single JSON", serialized: `{"type":"single"}`, preferred: []uint32{1, 2, 3}, usesConfiguredEndpoint: true},
		{name: "round robin", serialized: `round_robin`, preferred: []uint32{1, 2, 3}},
		{name: "round robin JSON", serialized: `{"type":"round_robin"}`, preferred: []uint32{1, 2, 3}},
		{name: "random choice", serialized: `random_choice`, preferred: []uint32{1, 2, 3}},
		{name: "random choice JSON", serialized: `{"type":"random_choice"}`, preferred: []uint32{1, 2, 3}},
		{
			name:       "legacy local DC",
			serialized: `{"type":"random_choice","prefer":"local_dc"}`,
			preferred:  []uint32{1},
			nearestDC:  true,
		},
		{
			name:       "nearest DC",
			serialized: `{"type":"random_choice","prefer":"nearest_dc"}`,
			preferred:  []uint32{1},
			nearestDC:  true,
		},
		{
			name:       "legacy local DC with fallback",
			serialized: `{"type":"random_choice","prefer":"local_dc","fallback":true}`,
			preferred:  []uint32{1},
			fallback:   []uint32{2, 3},
			nearestDC:  true,
		},
		{
			name:       "nearest DC with fallback",
			serialized: `{"type":"random_choice","prefer":"nearest_dc","fallback":true}`,
			preferred:  []uint32{1},
			fallback:   []uint32{2, 3},
			nearestDC:  true,
		},
		{
			name:       "locations",
			serialized: `{"type":"random_choice","prefer":"locations","locations":["a","c"]}`,
			preferred:  []uint32{1, 3},
		},
		{
			name:       "locations with fallback",
			serialized: `{"type":"random_choice","prefer":"locations","locations":["a","c"],"fallback":true}`,
			preferred:  []uint32{1, 3},
			fallback:   []uint32{2},
		},
		{
			name:       "unknown preference remains random choice",
			serialized: `{"type":"random_choice","prefer":"unknown"}`,
			preferred:  []uint32{1, 2, 3},
		},
	}
	endpoints := []endpoint.Endpoint{
		endpoint.New("a", endpoint.WithID(1), endpoint.WithLocation("a")),
		endpoint.New("b", endpoint.WithID(2), endpoint.WithLocation("b")),
		endpoint.New("c", endpoint.WithID(3), endpoint.WithLocation("c")),
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy := FromConfig(test.serialized)
			preferred, fallback := logicalGroups(policy, endpoints, strategy.Info{SelfLocation: "a"})

			require.Equal(t, test.preferred, preferred)
			require.Equal(t, test.fallback, fallback)
			require.Equal(t, test.usesConfiguredEndpoint, strategy.UsesConfiguredEndpoint(policy))
			require.Equal(t, test.nearestDC, strategy.DetectsNearestDC(policy))
		})
	}
}

func logicalGroups(
	estimator strategy.Estimator,
	endpoints []endpoint.Endpoint,
	info strategy.Info,
) (preferred, fallback []uint32) {
	nodeIDByKey := make(map[endpoint.Key]uint32, len(endpoints))
	for _, candidate := range endpoints {
		nodeIDByKey[candidate.Key()] = candidate.NodeID()
	}
	estimates := estimator.Estimate(info, endpoints)
	if len(estimates) == 0 {
		return nil, nil
	}
	minimum := estimates[0].Penalty
	for _, estimation := range estimates[1:] {
		minimum = min(minimum, estimation.Penalty)
	}
	for _, estimation := range estimates {
		if estimation.Penalty == minimum {
			preferred = append(preferred, nodeIDByKey[estimation.Key])
		} else {
			fallback = append(fallback, nodeIDByKey[estimation.Key])
		}
	}

	return preferred, fallback
}
