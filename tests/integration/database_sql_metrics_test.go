//go:build integration
// +build integration

package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xslices"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestDatabaseSqlMetrics(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		scope    = newScope(t)
		registry = newRegistryConfig(trace.DatabaseSQLEvents)
		db       = scope.SQLDriver(ydb.WithDatabaseSQLTrace(metrics.DatabaseSQL(registry)))
	)

	require.Equal(t,
		[]string{"database.sql.conns{}", "database.sql.tx{}"},
		xslices.Keys(registry.gauges.data),
	)
	require.NotNil(t, registry.gauges.data["database.sql.conns{}"].gauges)

	cc1, err := db.Conn(ctx)
	require.NoError(t, err)
	require.NotNil(t, cc1)
	registry.gauges.AssertEqual(t, "database.sql.conns", 1)
	require.Empty(t, registry.gauges.data["database.sql.tx{}"].gauges)

	cc2, err := db.Conn(ctx)
	require.NoError(t, err)
	require.NotNil(t, cc2)
	registry.gauges.AssertEqual(t, "database.sql.conns", 2)
	require.Empty(t, registry.gauges.data["database.sql.tx{}"].gauges)

	tx1, err := cc1.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, tx1)
	require.NotEmpty(t, registry.gauges.data["database.sql.tx{}"].gauges)
	registry.gauges.AssertEqual(t, "database.sql.tx", 1)

	require.NoError(t, tx1.Commit())
	registry.gauges.AssertEqual(t, "database.sql.tx", 0)

	require.NoError(t, cc1.Close())
	registry.gauges.AssertEqual(t, "database.sql.conns", 2)

	tx2, err := cc2.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, tx2)
	require.NotEmpty(t, registry.gauges.data["database.sql.tx{}"].gauges)
	registry.gauges.AssertEqual(t, "database.sql.tx", 1)

	require.NoError(t, tx2.Rollback())
	registry.gauges.AssertEqual(t, "database.sql.tx", 0)

	require.NoError(t, cc2.Close())
	registry.gauges.AssertEqual(t, "database.sql.conns", 2)

	require.NoError(t, db.Close())
	registry.gauges.AssertEqual(t, "database.sql.conns", 0)
}
