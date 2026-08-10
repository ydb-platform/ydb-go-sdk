package ydb_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
)

func TestUserBalancerConfigurationExpressionsCompile(t *testing.T) {
	options := []ydb.Option{
		ydb.WithBalancer(balancers.RandomChoice()),
		ydb.WithBalancer(balancers.SingleConn()),
		ydb.WithBalancer(balancers.PreferNearestDCWithFallBack(
			balancers.RandomChoice(),
		)),
		ydb.WithBalancer(balancers.PreferLocationsWithFallback(
			balancers.RandomChoice(), "a", "b",
		)),
	}

	require.Len(t, options, 4)
}
