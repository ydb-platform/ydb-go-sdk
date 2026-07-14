package ydb

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	internalDiscovery "github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery"
	discoveryConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
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

func TestDriverDiscoveryInitFailsWhenPoolClosed(t *testing.T) {
	ctx := context.Background()
	pool := conn.NewPool(ctx, config.New())
	require.NoError(t, pool.RemoveRef(ctx))

	d := &Driver{
		config: config.New(config.WithEndpoint("bootstrap:2135")),
		pool:   pool,
	}
	d.discovery = xsync.OnceValue(func() (*driverDiscoveryClient, error) {
		bootstrap := d.pool.Get(endpoint.New(d.config.Endpoint()))
		if bootstrap == nil {
			return nil, fmt.Errorf("discovery bootstrap connection: %w", conn.ErrClosedPool)
		}

		return &driverDiscoveryClient{
			Client: internalDiscovery.New(ctx, bootstrap, discoveryConfig.New()),
			pool:   d.pool,
			conn:   bootstrap,
		}, nil
	})

	_, err := d.discovery.Get()
	require.Error(t, err)
	require.ErrorIs(t, err, conn.ErrClosedPool)
}
