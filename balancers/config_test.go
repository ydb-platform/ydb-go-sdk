package balancers

import (
	"testing"

	"github.com/stretchr/testify/require"
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
