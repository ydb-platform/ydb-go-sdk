package balancers

import (
	"testing"

	"github.com/stretchr/testify/require"

	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

func TestFromConfig(t *testing.T) {
	for _, test := range []struct {
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
			name: "single",
			config: `{
				"type": "single"
			}`,
			res: balancerConfig.Config{SingleConn: true},
		},
		{
			name: "round_robin",
			config: `{
				"type": "round_robin"
			}`,
			res: balancerConfig.Config{
				RoundRobin: true,
			},
		},
		{
			name: "random_choice",
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
				DetectlocalDC: true,
				IsPreferConn: func(info balancerConfig.Info, c conn.Conn) bool {
					// some non nil func
					return false
				},
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
				AllowFalback:  true,
				DetectlocalDC: true,
				IsPreferConn: func(info balancerConfig.Info, c conn.Conn) bool {
					// some non nil func
					return false
				},
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
				IsPreferConn: func(info balancerConfig.Info, c conn.Conn) bool {
					// some non nil func
					return false
				},
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
				AllowFalback: true,
				IsPreferConn: func(info balancerConfig.Info, c conn.Conn) bool {
					// some non nil func
					return false
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var (
				actErr   error
				fallback = &balancerConfig.Config{}
			)
			b := FromConfig(
				test.config,
				WithParseErrorFallbackBalancer(fallback),
				WithParseErrorHandler(func(err error) {
					actErr = err
				}),
			)
			if test.fail && actErr == nil {
				t.Fatalf("expected error, but it not hanled")
			}
			if !test.fail && actErr != nil {
				t.Fatalf("unexpected error: %v", actErr)
			}
			if test.fail && b != fallback {
				t.Fatalf("unexpected balancer: %v", b)
			}

			// function pointers can check equal to nil only
			if test.res.IsPreferConn != nil {
				require.NotNil(t, b.IsPreferConn)
				b.IsPreferConn = nil
				test.res.IsPreferConn = nil
			}

			require.Equal(t, &test.res, b)
		})
	}
}
