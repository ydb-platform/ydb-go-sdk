package ydb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	internalDiscovery "github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery"
	discoveryConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
)

func TestDriverDiscoveryClientCloseReleasesBootstrapRef(t *testing.T) {
	ctx := context.Background()
	pool := conn.NewPool(ctx, config.New())
	defer func() {
		_ = pool.RemoveRef(ctx)
	}()

	e := endpoint.New("bootstrap:2135")
	bootstrap := pool.Get(e)
	require.NotNil(t, bootstrap)

	client := internalDiscovery.New(ctx, bootstrap, discoveryConfig.New())
	wrapper := &driverDiscoveryClient{
		Client: client,
		pool:   pool,
		conn:   bootstrap,
	}

	require.NoError(t, wrapper.Close(ctx))

	again := pool.Get(e)
	require.NotNil(t, again)
	require.NotSame(t, bootstrap, again)
}

func TestDriverConnectInitializesDiscoveryClient(t *testing.T) {
	ctx := context.Background()
	cfg := config.New(
		config.WithEndpoint("bootstrap:2135"),
		config.WithDatabase("/local"),
		config.WithBalancer(balancers.SingleConn()),
	)
	pool := conn.NewPool(ctx, cfg)

	d := &Driver{
		config: cfg,
		pool:   pool,
		metaBalancer: &balancerWithMeta{
			meta: cfg.Meta(),
			close: func(context.Context) error {
				return nil
			},
		},
	}

	require.NoError(t, d.connect(ctx))
	t.Cleanup(func() {
		require.NoError(t, d.metaBalancer.Close(ctx))
	})

	discoveryClient, err := d.discovery.Get()
	require.NoError(t, err)
	require.NotNil(t, discoveryClient)
	require.NotNil(t, discoveryClient.conn)

	require.NoError(t, discoveryClient.Close(ctx))
	require.NoError(t, pool.RemoveRef(ctx))
}

func TestDriverDiscoveryInitFailsWhenPoolClosed(t *testing.T) {
	ctx := context.Background()
	cfg := config.New(
		config.WithEndpoint("bootstrap:2135"),
		config.WithDatabase("/local"),
		config.WithBalancer(balancers.SingleConn()),
	)
	d, err := driverFromOptions(ctx,
		WithEndpoint(cfg.Endpoint()),
		WithDatabase(cfg.Database()),
		WithBalancer(balancers.SingleConn()),
	)
	require.NoError(t, err)

	require.NoError(t, d.connect(ctx))
	require.NoError(t, d.pool.RemoveRef(ctx))

	_, err = d.discovery.Get()
	require.Error(t, err)
	require.ErrorIs(t, err, conn.ErrClosedPool)
}
