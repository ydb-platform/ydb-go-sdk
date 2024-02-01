package trace

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDetailsMatch(t *testing.T) {
	var testCases []struct {
		name    string
		pattern string
		details Details
	}

	testCases = append(testCases, getDriverRelatedEventMatchTestCases()...)
	testCases = append(testCases, getYdbComponentEventMatchTestCases()...)
	testCases = append(testCases, getComplexYdbEventPatternsTestCases()...)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			details := MatchDetails(tc.pattern)
			require.Equal(t, tc.details.String(), details.String())
		})
	}
}

func getDriverRelatedEventMatchTestCases() []struct {
	name    string
	pattern string
	details Details
} {
	return []struct {
		name    string
		pattern string
		details Details
	}{
		{
			name:    "Match Driver Event",
			pattern: `^ydb\.driver$`,
			details: DriverEvents,
		},
		{
			name:    "Match All Driver Events",
			pattern: `^ydb\.driver\..*$`,
			details: DriverEvents,
		},
		{
			name:    "Match Driver Resolver Event",
			pattern: `^ydb\.driver\.resolver$`,
			details: DriverResolverEvents,
		},
		{
			name:    "Match Driver Conn, Credentials, and Resolver Events",
			pattern: `^ydb\.driver\.(conn|credentials|resolver)$`,
			details: DriverConnEvents | DriverCredentialsEvents | DriverResolverEvents,
		},
	}
}

func getYdbComponentEventMatchTestCases() []struct {
	name    string
	pattern string
	details Details
} {
	return []struct {
		name    string
		pattern string
		details Details
	}{
		{
			name:    "Match Scheme Events",
			pattern: `^ydb\.scheme$`,
			details: SchemeEvents,
		},
		{
			name:    "Match Table Events",
			pattern: `^ydb\.table`,
			details: TableEvents,
		},
		{
			name:    "Match Scripting Events",
			pattern: `^ydb\.scripting$`,
			details: ScriptingEvents,
		},
		{
			name:    "Match Coordination Events",
			pattern: `^ydb\.coordination$`,
			details: CoordinationEvents,
		},
		{
			name:    "Match Ratelimiter Events",
			pattern: `^ydb\.ratelimiter$`,
			details: RatelimiterEvents,
		},
		{
			name:    "Match Retry Events",
			pattern: `^ydb\.retry$`,
			details: RetryEvents,
		},
		{
			name:    "Match Discovery Events",
			pattern: `^ydb\.discovery$`,
			details: DiscoveryEvents,
		},
	}
}

func getComplexYdbEventPatternsTestCases() []struct {
	name    string
	pattern string
	details Details
} {
	return []struct {
		name    string
		pattern string
		details Details
	}{
		{
			name:    "Match Combined Driver, Discovery, Retry, Table, and Scheme Events",
			pattern: `^ydb\.(driver|discovery|retry|table|scheme).*$`,
			details: DriverEvents | DiscoveryEvents | RetryEvents | TableEvents | SchemeEvents,
		},
		{
			name:    "Match Table Pool, Session LifeCycle, API, and Session Events",
			pattern: `^ydb\.table\.(pool\.(session|api)|session).*$`,
			details: TablePoolSessionLifeCycleEvents | TablePoolAPIEvents | TableSessionEvents,
		},
		{
			name:    "Match Combined Database SQL Tx, Driver Balancer, Conn, Table Pool, and Retry Events",
			pattern: `^ydb\.((database.sql.tx)|driver.(balancer|conn)|(table\.pool)|retry)$`,
			details: DriverBalancerEvents | DriverConnEvents | TablePoolLifeCycleEvents | DatabaseSQLTxEvents | RetryEvents,
		},
	}
}
