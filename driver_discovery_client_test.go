package ydb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	internalDiscovery "github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery"
	discoveryConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery/config"
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
	require.NoError(t, client.Close(ctx))

	again := pool.Get(e)
	require.NotNil(t, again)
	require.NotSame(t, bootstrap, again)
}
