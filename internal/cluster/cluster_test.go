package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/multi"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

func TestNew(t *testing.T) {
	ctx := context.Background()

	cfg := config.New(config.WithBalancer(balancers.RoundRobin()))
	cluster := New(ctx, cfg, conn.NewPool(ctx, cfg), nil)
	require.IsType(t, multi.Balancer(), cluster.balancer())
}
