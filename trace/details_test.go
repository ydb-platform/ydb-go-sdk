package trace

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDetailsMatch(t *testing.T) {
	for _, tt := range []struct {
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
			pattern: `^ydb\.query`,
			details: QueryEvents,
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
		{
			pattern: `^ydb\.query\.(pool|session|tx|result).*$`,
			details: QueryPoolEvents | QuerySessionEvents | QueryTransactionEvents | QueryResultEvents,
		},
		{
			pattern: `^ydb\.((database.sql.tx)|driver.(balancer|conn)|(table\.pool)|retry)$`,
			details: DriverBalancerEvents | DriverConnEvents | TablePoolLifeCycleEvents | DatabaseSQLTxEvents | RetryEvents,
		},
	} {
		t.Run("", func(t *testing.T) {
			details := MatchDetails(tt.pattern)
			require.Equal(t, tt.details.String(), details.String())
		})
	}
}
