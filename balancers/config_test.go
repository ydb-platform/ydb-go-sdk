package balancers

import (
	"testing"

	"github.com/stretchr/testify/require"

	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
)

func TestFromConfig(t *testing.T) {
	for _, tt := range []struct {
		name   string
		config string
		res    balancerConfig.Config
		fail   bool
	}{
		{
			name:   "empty",
			config: ``,
			res:    balancerConfig.Config{},
			fail:   true,
		},
		{
			name:   "disable",
			config: `disable`,
			res:    balancerConfig.Config{SingleConn: true},
		},
		{
			name:   "single",
			config: `single`,
			res:    balancerConfig.Config{SingleConn: true},
		},
		{
			name: "single/JSON",
			config: `{
				"type": "single"
			}`,
			res: balancerConfig.Config{SingleConn: true},
		},
		{
			name:   "round_robin",
			config: `round_robin`,
			res:    balancerConfig.Config{},
		},
		{
			name: "round_robin/JSON",
			config: `{
				"type": "round_robin"
			}`,
			res: balancerConfig.Config{},
		},
		{
			name:   "random_choice",
			config: `random_choice`,
			res:    balancerConfig.Config{},
		},
		{
			name: "random_choice/JSON",
			config: `{
				"type": "random_choice"
			}`,
			res: balancerConfig.Config{},
		},
		{
			name: "prefer_local_dc",
			config: `{
				"type": "random_choice",
				"prefer": "local_dc"
			}`,
			res: balancerConfig.Config{
				DetectNearestDC: true,
				Filter: filterFunc(func(info balancerConfig.Info, e endpoint.Info) bool {
					// some non nil func
					return false
				}),
			},
		},
		{
			name: "prefer_nearest_dc",
			config: `{
				"type": "random_choice",
				"prefer": "nearest_dc"
			}`,
			res: balancerConfig.Config{
				DetectNearestDC: true,
				Filter: filterFunc(func(info balancerConfig.Info, e endpoint.Info) bool {
					// some non nil func
					return false
				}),
			},
		},
		{
			name: "prefer_unknown_type",
			config: `{
				"type": "unknown_type",
				"prefer": "local_dc"
			}`,
			fail: true,
		},
		{
			name: "prefer_local_dc_with_fallback",
			config: `{
				"type": "random_choice",
				"prefer": "local_dc",
				"fallback": true
			}`,
			res: balancerConfig.Config{
				AllowFallback:   true,
				DetectNearestDC: true,
				Filter: filterFunc(func(info balancerConfig.Info, e endpoint.Info) bool {
					// some non nil func
					return false
				}),
			},
		},
		{
			name: "prefer_nearest_dc_with_fallback",
			config: `{
				"type": "random_choice",
				"prefer": "nearest_dc",
				"fallback": true
			}`,
			res: balancerConfig.Config{
				AllowFallback:   true,
				DetectNearestDC: true,
				Filter: filterFunc(func(info balancerConfig.Info, e endpoint.Info) bool {
					// some non nil func
					return false
				}),
			},
		},
		{
			name: "prefer_locations",
			config: `{
				"type": "random_choice",
				"prefer": "locations",
				"locations": ["AAA", "BBB", "CCC"]
			}`,
			res: balancerConfig.Config{
				Filter: filterFunc(func(info balancerConfig.Info, e endpoint.Info) bool {
					// some non nil func
					return false
				}),
			},
		},
		{
			name: "prefer_locations_with_fallback",
			config: `{
				"type": "random_choice",
				"prefer": "locations",
				"locations": ["AAA", "BBB", "CCC"],
				"fallback": true
			}`,
			res: balancerConfig.Config{
				AllowFallback: true,
				Filter: filterFunc(func(info balancerConfig.Info, e endpoint.Info) bool {
					// some non nil func
					return false
				}),
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var (
				actErr   error
				fallback = &balancerConfig.Config{}
			)
			b := FromConfig(
				tt.config,
				WithParseErrorFallbackBalancer(fallback),
				WithParseErrorHandler(func(err error) {
					actErr = err
				}),
			)
			if tt.fail && actErr == nil {
				t.Fatalf("expected error, but it not hanled")
			}
			if !tt.fail && actErr != nil {
				t.Fatalf("unexpected error: %v", actErr)
			}
			if tt.fail && b != fallback {
				t.Fatalf("unexpected balancer: %v", b)
			}

			// function pointers can check equal to nil only
			if tt.res.Filter != nil {
				require.NotNil(t, b.Filter)
				b.Filter = nil
				tt.res.Filter = nil
			}

			require.Equal(t, tt.res, *b)
		})
	}
}
