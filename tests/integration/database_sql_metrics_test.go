//go:build integration
// +build integration

package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xslices"
	"github.com/ydb-platform/ydb-go-sdk/v3/metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestDatabaseSqlMetrics(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		scope    = newScope(t)
		registry = &registryConfig{
			details:    trace.DatabaseSQLEvents,
			gauges:     newVec[gaugeVec](),
			counters:   newVec[counterVec](),
			timers:     newVec[timerVec](),
			histograms: newVec[histogramVec](),
		}
		db = scope.SQLDriver(ydb.WithDatabaseSQLTrace(metrics.DatabaseSQL(registry)))
	)

	require.Equal(t,
		[]string{"database.sql.conns{}", "database.sql.tx{}"},
		xslices.Keys(registry.gauges.data),
	)
	require.EqualValues(t, 1, registry.gauges.data["database.sql.conns{}"].gauges["database.sql.conns{}"].value)

	cc1, err := db.Conn(ctx)
	require.NoError(t, err)
	require.NotNil(t, cc1)
	require.EqualValues(t, 1, registry.gauges.data["database.sql.conns{}"].gauges["database.sql.conns{}"].value)
	require.Empty(t, registry.gauges.data["database.sql.tx{}"].gauges)

	cc2, err := db.Conn(ctx)
	require.NoError(t, err)
	require.NotNil(t, cc2)
	require.EqualValues(t, 2, registry.gauges.data["database.sql.conns{}"].gauges["database.sql.conns{}"].value)
	require.Empty(t, registry.gauges.data["database.sql.tx{}"].gauges)

	tx1, err := cc1.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, tx1)
	require.NotEmpty(t, registry.gauges.data["database.sql.tx{}"].gauges)
	require.EqualValues(t, 1, registry.gauges.data["database.sql.tx{}"].gauges["database.sql.tx{}"].value)

	require.NoError(t, tx1.Commit())
	require.EqualValues(t, 0, registry.gauges.data["database.sql.tx{}"].gauges["database.sql.tx{}"].value)

	require.NoError(t, cc1.Close())
	require.EqualValues(t, 2, registry.gauges.data["database.sql.conns{}"].gauges["database.sql.conns{}"].value)

	tx2, err := cc2.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, tx2)
	require.NotEmpty(t, registry.gauges.data["database.sql.tx{}"].gauges)
	require.EqualValues(t, 1, registry.gauges.data["database.sql.tx{}"].gauges["database.sql.tx{}"].value)

	require.NoError(t, tx2.Rollback())
	require.EqualValues(t, 0, registry.gauges.data["database.sql.tx{}"].gauges["database.sql.tx{}"].value)

	require.NoError(t, cc2.Close())
	require.EqualValues(t, 2, registry.gauges.data["database.sql.conns{}"].gauges["database.sql.conns{}"].value)

	require.NoError(t, db.Close())
	require.EqualValues(t, 0, registry.gauges.data["database.sql.conns{}"].gauges["database.sql.conns{}"].value)
}
