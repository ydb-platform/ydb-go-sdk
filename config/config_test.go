package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
)

func TestWithBalancer(t *testing.T) {
	balancer := balancers.PreferNearestDC(balancers.RandomChoice())
	cfg := New(WithBalancer(balancer))

	require.Equal(t, balancer.String(), cfg.Balancer().String())
}

func TestGrpcChannelShutdownTimeout(t *testing.T) {
	require.Equal(t, 119*time.Second, New().GrpcChannelShutdownTimeout())
	require.Equal(t, time.Second, New(WithGrpcChannelShutdownTimeout(time.Second)).GrpcChannelShutdownTimeout())
}
