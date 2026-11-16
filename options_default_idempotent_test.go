package ydb

import (
	"testing"

	"github.com/stretchr/testify/require"

	queryConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/query/config"
	tableConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql"
)

func TestWithDefaultIdempotent(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		driver, err := driverFromOptions(t.Context())
		require.NoError(t, err)

		require.False(t, queryConfig.New(driver.queryOptions...).DefaultIdempotent())
		require.False(t, tableConfig.New(driver.tableOptions...).DefaultIdempotent())

		connector := &xsql.Connector{}
		for _, opt := range driver.databaseSQLOptions {
			require.NoError(t, opt.Apply(connector))
		}
		require.False(t, connector.DefaultIdempotent())
	})

	t.Run("enabled", func(t *testing.T) {
		driver, err := driverFromOptions(t.Context(), WithDefaultIdempotent(true))
		require.NoError(t, err)

		require.True(t, queryConfig.New(driver.queryOptions...).DefaultIdempotent())
		require.True(t, tableConfig.New(driver.tableOptions...).DefaultIdempotent())

		connector := &xsql.Connector{}
		for _, opt := range driver.databaseSQLOptions {
			require.NoError(t, opt.Apply(connector))
		}
		require.True(t, connector.DefaultIdempotent())
	})
}
