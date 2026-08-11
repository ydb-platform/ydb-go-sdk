package balancer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/strategy"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	discoveryConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
)

func TestNewReturnsDiscoveryStartError(t *testing.T) {
	ctx := t.Context()
	expectedErr := errors.New("credentials failed")
	srv := startDynamicDiscoveryServer(t, []uint32{1})
	cfg := config.New(
		config.WithEndpoint(srv.endpoint()),
		config.WithDatabase("/local"),
		config.WithGrpcOptions(grpc.WithTransportCredentials(insecure.NewCredentials())),
		config.WithCredentials(errorCredentials{err: expectedErr}),
	)
	pool := conn.NewPool(ctx, cfg)
	t.Cleanup(func() {
		require.NoError(t, pool.RemoveRef(ctx))
	})

	balancer, err := New(ctx, cfg, pool, discoveryConfig.WithInterval(-time.Nanosecond))

	require.ErrorIs(t, err, expectedErr)
	require.Nil(t, balancer)
}

func TestClusterDiscoveryAttemptReturnsLocalDCDetectorError(t *testing.T) {
	expectedErr := errors.New("local DC detection failed")
	policy := strategy.PreferNearestDC(
		strategy.RandomChoice(), "LocalDC", func(strategy.Info, endpoint.Info) bool { return true }, false,
	)
	balancer := &Balancer{
		driverConfig:    config.New(),
		estimator:       policy,
		detectNearestDC: true,
		discover: func(context.Context, *grpc.ClientConn) ([]endpoint.Endpoint, string, error) {
			return []endpoint.Endpoint{endpoint.New("node:2135")}, "", nil
		},
		localDCDetector: func(context.Context, []endpoint.Endpoint) (string, error) {
			return "", expectedErr
		},
	}

	err := balancer.clusterDiscoveryAttempt(t.Context(), nil)

	require.ErrorIs(t, err, expectedErr)
}

type errorCredentials struct {
	err error
}

func (c errorCredentials) Token(context.Context) (string, error) {
	return "", c.err
}
