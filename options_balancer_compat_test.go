package ydb_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
)

func TestUserBalancerConfigurationExpressionsCompile(t *testing.T) {
	configured := balancers.RandomChoice()
	configured = balancers.PreferNearestDCWithFallBack(configured)
	configured = balancers.WithMaxConnections(configured, 9)

	options := []ydb.Option{
		ydb.WithBalancer(balancers.RandomChoice()),
		ydb.WithBalancer(balancers.SingleConn()),
		ydb.WithBalancer(balancers.PreferNearestDCWithFallBack(
			balancers.RandomChoice(),
		)),
		ydb.WithBalancer(balancers.PreferLocationsWithFallback(
			balancers.RandomChoice(), "a", "b",
		)),
		ydb.WithBalancer(balancers.WithMaxConnections(
			balancers.PreferNearestDCWithFallback(
				balancers.RandomChoice(),
			),
			9,
		)),
		ydb.WithBalancer(configured),
	}

	require.Len(t, options, 6)
}
