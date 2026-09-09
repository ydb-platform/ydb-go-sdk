package balancers

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/policy"
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
		{name: "round_robin", config: `round_robin`, expected: "Priority"},
		{name: "round_robin/JSON", config: `{"type":"round_robin"}`, expected: "Priority"},
		{name: "random_choice", config: `random_choice`, expected: "Priority"},
		{name: "random_choice/JSON", config: `{"type":"random_choice"}`, expected: "Priority"},
		{
			name:     "prefer_local_dc",
			config:   `{"type":"random_choice","prefer":"local_dc"}`,
			expected: "Priority{Preferences=[LocalDC]}",
		},
		{
			name:     "prefer_nearest_dc",
			config:   `{"type":"random_choice","prefer":"nearest_dc"}`,
			expected: "Priority{Preferences=[LocalDC]}",
		},
		{
			name:   "prefer_unknown_type",
			config: `{"type":"unknown_type","prefer":"local_dc"}`,
			fail:   true,
		},
		{
			name:     "unknown preference",
			config:   `{"type":"random_choice","prefer":"unknown"}`,
			expected: "Priority",
		},
		{
			name:     "prefer_local_dc_with_fallback",
			config:   `{"type":"random_choice","prefer":"local_dc","fallback":true}`,
			expected: "Priority{Preferences=[LocalDC(AllowFallback)]}",
		},
		{
			name:     "prefer_nearest_dc_with_fallback",
			config:   `{"type":"random_choice","prefer":"nearest_dc","fallback":true}`,
			expected: "Priority{Preferences=[LocalDC(AllowFallback)]}",
		},
		{
			name:     "prefer_locations",
			config:   `{"type":"random_choice","prefer":"locations","locations":["AAA","BBB","CCC"]}`,
			expected: "Priority{Preferences=[Locations{AAA,BBB,CCC}]}",
		},
		{
			name:     "prefer_locations_with_fallback",
			config:   `{"type":"random_choice","prefer":"locations","locations":["AAA","BBB","CCC"],"fallback":true}`,
			expected: "Priority{Preferences=[Locations{AAA,BBB,CCC}(AllowFallback)]}",
		},
		{
			name:     "prefer_primary_pile",
			config:   `{"type":"random_choice","prefer":"primary_pile"}`,
			expected: "Priority{Preferences=[PrimaryPile]}",
		},
		{
			name:     "prefer_primary_pile_with_fallback",
			config:   `{"type":"random_choice","prefer":"primary_pile","fallback":true}`,
			expected: "Priority{Preferences=[PrimaryPile(AllowFallback)]}",
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
		lowerPriority          []uint32
		excluded               []uint32
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
			excluded:   []uint32{2, 3},
			nearestDC:  true,
		},
		{
			name:       "nearest DC",
			serialized: `{"type":"random_choice","prefer":"nearest_dc"}`,
			preferred:  []uint32{1},
			excluded:   []uint32{2, 3},
			nearestDC:  true,
		},
		{
			name:          "legacy local DC with fallback",
			serialized:    `{"type":"random_choice","prefer":"local_dc","fallback":true}`,
			preferred:     []uint32{1},
			lowerPriority: []uint32{2, 3},
			nearestDC:     true,
		},
		{
			name:          "nearest DC with fallback",
			serialized:    `{"type":"random_choice","prefer":"nearest_dc","fallback":true}`,
			preferred:     []uint32{1},
			lowerPriority: []uint32{2, 3},
			nearestDC:     true,
		},
		{
			name:       "locations",
			serialized: `{"type":"random_choice","prefer":"locations","locations":["a","c"]}`,
			preferred:  []uint32{1, 3},
			excluded:   []uint32{2},
		},
		{
			name:          "locations with fallback",
			serialized:    `{"type":"random_choice","prefer":"locations","locations":["a","c"],"fallback":true}`,
			preferred:     []uint32{1, 3},
			lowerPriority: []uint32{2},
		},
		{
			name:       "primary pile",
			serialized: `{"type":"random_choice","prefer":"primary_pile"}`,
			preferred:  []uint32{1},
			excluded:   []uint32{2, 3},
		},
		{
			name:          "primary pile with fallback",
			serialized:    `{"type":"random_choice","prefer":"primary_pile","fallback":true}`,
			preferred:     []uint32{1},
			lowerPriority: []uint32{2, 3},
		},
		{
			name:       "unknown preference remains random choice",
			serialized: `{"type":"random_choice","prefer":"unknown"}`,
			preferred:  []uint32{1, 2, 3},
		},
	}
	endpoints := []endpoint.Endpoint{
		endpoint.New("a", endpoint.WithID(1), endpoint.WithLocation("a"), endpoint.WithMetadata(endpoint.Metadata{
			BridgePileState: endpoint.PileStatePrimary,
		})),
		endpoint.New("b", endpoint.WithID(2), endpoint.WithLocation("b"), endpoint.WithMetadata(endpoint.Metadata{
			BridgePileState: endpoint.PileStateSynchronized,
		})),
		endpoint.New("c", endpoint.WithID(3), endpoint.WithLocation("c")),
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			p := FromConfig(test.serialized)
			preferred, lowerPriority, excluded := logicalGroups(p, endpoints, policy.Info{SelfLocation: "a"})

			require.Equal(t, test.preferred, preferred)
			require.Equal(t, test.lowerPriority, lowerPriority)
			require.Equal(t, test.excluded, excluded)
			require.Equal(t, test.usesConfiguredEndpoint, p.SingleConnection())
			require.Equal(t, test.nearestDC, p.DetectsNearestDC())
		})
	}
}

func logicalGroups(
	policy policy.Policy,
	endpoints []endpoint.Endpoint,
	info policy.Info,
) (preferred, lowerPriority, excluded []uint32) {
	nodeIDByKey := make(map[endpoint.Key]uint32, len(endpoints))
	for _, candidate := range endpoints {
		nodeIDByKey[candidate.Key()] = candidate.NodeID()
	}
	priorities := policy.Prioritize(info, endpoints)
	minimum := ^uint64(0)
	for _, candidate := range priorities {
		if !candidate.Excluded {
			minimum = min(minimum, candidate.Priority)
		}
	}
	for _, candidate := range priorities {
		switch {
		case candidate.Excluded:
			excluded = append(excluded, nodeIDByKey[candidate.Key])
		case candidate.Priority == minimum:
			preferred = append(preferred, nodeIDByKey[candidate.Key])
		default:
			lowerPriority = append(lowerPriority, nodeIDByKey[candidate.Key])
		}
	}

	return preferred, lowerPriority, excluded
}
