package config

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
)

func TestWithBalancer(t *testing.T) {
	balancer := balancers.PreferNearestDC(balancers.RandomChoice())
	cfg := New(WithBalancer(balancer))

	require.Equal(t, balancer, cfg.Balancer())
}
