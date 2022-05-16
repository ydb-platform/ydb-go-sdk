package balancers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

type testBalancer struct{}

func (t testBalancer) Next(ctx context.Context, opts ...balancer.NextOption) conn.Conn {
	panic("unexpected call")
}

func (t testBalancer) Create([]conn.Conn) balancer.Balancer {
	panic("unexpected call")
}

func TestFromConfig(t *testing.T) {
	for _, test := range []struct {
		name   string
		config string
		res    balancer.Config
		fail   bool
	}{
		{
			name:   "empty",
			config: ``,
			res:    balancer.Config{},
			fail:   true,
		},
		{
			name: "single",
			config: `{
				"type": "single"
			}`,
			res: balancer.Config{SingleConn: true},
		},
		{
			name: "round_robin",
			config: `{
				"type": "round_robin"
			}`,
			res: balancer.Config{},
		},
		{
			name: "random_choice",
			config: `{
				"type": "random_choice"
			}`,
			res: balancer.Config{},
		},
		{
			name: "prefer_local_dc",
			config: `{
				"type": "random_choice",
				"prefer": "local_dc"
			}`,
			res: balancer.Config{IsPreferConn: func(c conn.Conn) bool {
				// some non nil func
				return false
			}},
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
			res: balancer.Config{
				AllowFalback: true,
				IsPreferConn: func(c conn.Conn) bool {
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
			res: balancer.Config{
				IsPreferConn: func(c conn.Conn) bool {
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
			res: balancer.Config{
				AllowFalback: true,
				IsPreferConn: func(c conn.Conn) bool {
					// some non nil func
					return false
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var (
				actErr   error
				fallback = &balancer.Config{}
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
