//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestQueryPoolMetricsWhileSessionIsCheckedOut(t *testing.T) {
	// Regression: https://github.com/ydb-platform/ydb-go-sdk/issues/2295
	scope := newScope(t)

	registry := newRegistryConfig(trace.QueryPoolEvents)
	db := scope.Driver(
		ydb.WithSessionPoolSizeLimit(1),
		metrics.WithTraces(registry),
	)

	require.NoError(t, db.Query().Do(scope.Ctx, func(ctx context.Context, s query.Session) error {
		if err := s.Exec(ctx, "SELECT 1"); err != nil {
			return err
		}

		// Do retains the session until this callback returns, even after Exec completes.
		// Inspect the actual metrics adapter output while the only pool slot is occupied.
		registry.gauges.AssertEqual(t, "ydb.query.pool.size.in_use", 1)

		return nil
	}, query.WithIdempotent()))

	require.NoError(t, db.Query().Do(scope.Ctx, func(ctx context.Context, s query.Session) error {
		if err := s.Exec(ctx, "SELECT 1"); err != nil {
			return err
		}

		// The reused session is checked out again and must no longer be counted as idle.
		registry.gauges.AssertEqual(t, "ydb.query.pool.size.idle", 0)

		return nil
	}, query.WithIdempotent()))
}
