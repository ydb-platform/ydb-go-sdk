package ydb_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
)

func TestUserBalancerConfigurationExpressionsCompile(t *testing.T) {
	configured := balancers.RandomChoice()
	configured = balancers.PreferNearestDCWithFallBack(configured)
	configured = balancers.PreferLocationsWithFallback(configured, "a", "b")

	fromConfig, err := balancers.CreateFromConfig("random_choice")
	require.NoError(t, err)

	options := []ydb.Option{
		ydb.WithBalancer(balancers.RoundRobin()), //nolint:staticcheck // Verify deprecated API compatibility.
		ydb.WithBalancer(balancers.RandomChoice()),
		ydb.WithBalancer(balancers.SingleConn()),
		ydb.WithBalancer(balancers.Default()),
		ydb.WithBalancer(balancers.PreferLocalDC( //nolint:staticcheck // Verify deprecated API compatibility.
			balancers.RandomChoice(),
		)),
		ydb.WithBalancer(balancers.PreferLocalDCWithFallBack( //nolint:staticcheck // Verify deprecated API compatibility.
			balancers.RandomChoice(),
		)),
		ydb.WithBalancer(balancers.PreferNearestDC(
			balancers.RandomChoice(),
		)),
		ydb.WithBalancer(balancers.PreferNearestDCWithFallBack(
			balancers.RandomChoice(),
		)),
		ydb.WithBalancer(balancers.PreferLocations(
			balancers.RandomChoice(), "a", "b",
		)),
		ydb.WithBalancer(balancers.PreferLocationsWithFallback(
			balancers.RandomChoice(), "a", "b",
		)),
		ydb.WithBalancer(balancers.Prefer(
			balancers.RandomChoice(), func(endpoint balancers.Endpoint) bool {
				return endpoint.NodeID() == 1
			},
		)),
		ydb.WithBalancer(balancers.PreferWithFallback(
			balancers.RandomChoice(), func(endpoint balancers.Endpoint) bool {
				return endpoint.NodeID() == 1
			},
		)),
		ydb.WithBalancer(fromConfig),
		ydb.WithBalancer(balancers.FromConfig(
			"invalid",
			balancers.WithParseErrorFallbackBalancer(balancers.RandomChoice()),
		)),
		ydb.WithBalancer(configured),
	}

	require.Len(t, options, 15)

	configOptions := []config.Option{
		config.WithBalancer(balancers.RandomChoice()),
		config.WithBalancer(configured),
	}
	require.Len(t, configOptions, 2)
}
