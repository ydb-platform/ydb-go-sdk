package trace

import (
	"testing"
)

func TestDetailsMatch(t *testing.T) {
	for _, test := range []struct {
		pattern string
		details Details
	}{
		{
			pattern: `^ydb\.driver$`,
			details: DriverEvents,
		},
		{
			pattern: `^ydb\.driver\..*$`,
			details: DriverEvents,
		},
		{
			pattern: `^ydb\.driver\.resolver$`,
			details: DriverResolverEvents,
		},
		{
			pattern: `^ydb\.driver\.(conn|credentials|resolver)$`,
			details: DriverConnEvents | DriverCredentialsEvents | DriverResolverEvents,
		},
		{
			pattern: `^ydb\.scheme$`,
			details: SchemeEvents,
		},
		{
			pattern: `^ydb\.table`,
			details: TableEvents,
		},
		{
			pattern: `^ydb\.scripting$`,
			details: ScriptingEvents,
		},
		{
			pattern: `^ydb\.coordination$`,
			details: CoordinationEvents,
		},
		{
			pattern: `^ydb\.ratelimiter$`,
			details: RatelimiterEvents,
		},
		{
			pattern: `^ydb\.retry$`,
			details: RetryEvents,
		},
		{
			pattern: `^ydb\.discovery$`,
			details: DiscoveryEvents,
		},
		{
			pattern: `^ydb\.(driver|discovery|retry|table|scheme).*$`,
			details: DriverEvents | DiscoveryEvents | RetryEvents | TableEvents | SchemeEvents,
		},
		{
			pattern: `^ydb\.table\.(pool\.(session|api)|session).*$`,
			details: TablePoolSessionLifeCycleEvents | TablePoolAPIEvents | TableSessionEvents,
		},
	} {
		t.Run(test.pattern, func(t *testing.T) {
			if MatchDetails(test.pattern) != test.details {
				t.Fatalf(
					"unexpected match details by pattern '%s': %d, exp %d",
					test.pattern,
					MatchDetails(test.pattern),
					test.details,
				)
			}
		})
	}
}
