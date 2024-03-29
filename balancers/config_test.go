package balancers

import (
	"testing"

	"github.com/stretchr/testify/require"

	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

func TestFromConfig(t *testing.T) {
	for _, tt := range []struct {
		name   string
		config string
		res    *balancerConfig.Config
		fail   bool
	}{
		{
			name:   "empty",
			config: ``,
			res:    balancerConfig.New(),
			fail:   true,
		},
		{
			name:   "disable",
			config: `disable`,
			res:    balancerConfig.New(balancerConfig.UseSingleConn()),
		},
		{
			name:   "single",
			config: `single`,
			res:    balancerConfig.New(balancerConfig.UseSingleConn()),
		},
		{
			name: "single/JSON",
			config: `{
				"type": "single"
			}`,
			res: balancerConfig.New(balancerConfig.UseSingleConn()),
		},
		{
			name:   "round_robin",
			config: `round_robin`,
			res:    balancerConfig.New(),
		},
		{
			name: "round_robin/JSON",
			config: `{
				"type": "round_robin"
			}`,
			res: balancerConfig.New(),
		},
		{
			name:   "random_choice",
			config: `random_choice`,
			res:    balancerConfig.New(),
		},
		{
			name: "random_choice/JSON",
			config: `{
				"type": "random_choice"
			}`,
			res: balancerConfig.New(),
		},
		{
			name: "prefer_local_dc",
			config: `{
				"type": "random_choice",
				"prefer": "local_dc"
			}`,
			res: balancerConfig.New(
				balancerConfig.DetectLocalDC(),
				balancerConfig.FilterFunc(func(info balancerConfig.Info, c conn.Info) bool {
					// some non nil func
					return false
				}),
			),
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
			res: balancerConfig.New(
				balancerConfig.AllowFallback(),
				balancerConfig.DetectLocalDC(),
				balancerConfig.FilterFunc(func(info balancerConfig.Info, c conn.Info) bool {
					// some non nil func
					return false
				}),
			),
		},
		{
			name: "prefer_locations",
			config: `{
				"type": "random_choice",
				"prefer": "locations",
				"locations": ["AAA", "BBB", "CCC"]
			}`,
			res: balancerConfig.New(
				balancerConfig.FilterFunc(func(info balancerConfig.Info, c conn.Info) bool {
					// some non nil func
					return false
				}),
			),
		},
		{
			name: "prefer_locations_with_fallback",
			config: `{
				"type": "random_choice",
				"prefer": "locations",
				"locations": ["AAA", "BBB", "CCC"],
				"fallback": true
			}`,
			res: balancerConfig.New(
				balancerConfig.AllowFallback(),
				balancerConfig.FilterFunc(func(info balancerConfig.Info, c conn.Info) bool {
					// some non nil func
					return false
				}),
			),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var (
				actErr   error
				fallback = balancerConfig.New()
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
			if tt.res.Filter() != nil {
				require.NotNil(t, b.Filter())
				b = b.With(balancerConfig.FilterFunc(nil))
				tt.res = tt.res.With(balancerConfig.FilterFunc(nil))
			}

			require.Equal(t, tt.res, b)
		})
	}
}
