//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	baseTx "github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestDatabaseSqlWithLazyTx(t *testing.T) {
	scope := newScope(t)

	newDB := func(t *testing.T, lazyTxDetected *atomic.Bool, driverOpts ...ydb.Option) *sql.DB {
		baseOpts := []ydb.Option{
			ydb.WithQueryService(true),
			ydb.WithTraceQuery(trace.Query{
				OnSessionBegin: func(info trace.QuerySessionBeginStartInfo) func(trace.QuerySessionBeginDoneInfo) {
					return func(info trace.QuerySessionBeginDoneInfo) {
						if info.Error == nil && info.Tx != nil && info.Tx.ID() == baseTx.LazyTxID {
							lazyTxDetected.Store(true)
						}
					}
				},
			}),
		}

		driver := scope.Driver(append(baseOpts, driverOpts...)...)

		connector, err := ydb.Connector(driver,
			ydb.WithTablePathPrefix(scope.Folder()),
			ydb.WithAutoDeclare(),
		)
		require.NoError(t, err)

		return sql.OpenDB(connector)
	}

	t.Run("WithLazyTxTrue", func(t *testing.T) {
		var lazyTxDetected atomic.Bool
		db := newDB(t, &lazyTxDetected)
		defer func() { _ = db.Close() }()

		require.NoError(t, retry.DoTx(scope.Ctx, db, func(ctx context.Context, tx *sql.Tx) error {
			rows, err := tx.QueryContext(ctx, "SELECT 1")
			if err != nil {
				return err
			}
			defer func() { _ = rows.Close() }()

			for rows.Next() {
				var v int
				if err := rows.Scan(&v); err != nil {
					return err
				}
			}

			return rows.Err()
		}, retry.WithLazyTx(true)))

		require.True(t, lazyTxDetected.Load(), "lazy transaction should be detected via trace")
	})

	t.Run("WithLazyTxFalse", func(t *testing.T) {
		var lazyTxDetected atomic.Bool
		db := newDB(t, &lazyTxDetected)
		defer func() { _ = db.Close() }()

		require.NoError(t, retry.DoTx(scope.Ctx, db, func(ctx context.Context, tx *sql.Tx) error {
			rows, err := tx.QueryContext(ctx, "SELECT 1")
			if err != nil {
				return err
			}
			defer func() { _ = rows.Close() }()

			for rows.Next() {
				var v int
				if err := rows.Scan(&v); err != nil {
					return err
				}
			}

			return rows.Err()
		}, retry.WithLazyTx(false)))

		require.False(t, lazyTxDetected.Load(), "lazy transaction should NOT be detected when WithLazyTx(false)")
	})

	t.Run("WithLazyTxTrueOverridesDriverDefault", func(t *testing.T) {
		var lazyTxDetected atomic.Bool
		db := newDB(t, &lazyTxDetected) // Driver without global lazyTx
		defer func() { _ = db.Close() }()

		require.NoError(t, retry.DoTx(scope.Ctx, db, func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, "SELECT 1")

			return err
		}, retry.WithLazyTx(true)))

		require.True(t, lazyTxDetected.Load(), "lazy transaction should be enabled via retry.WithLazyTx(true)")
	})

	t.Run("WithLazyTxFalseOverridesDriverLazyTx", func(t *testing.T) {
		var lazyTxDetected atomic.Bool
		db := newDB(t, &lazyTxDetected, ydb.WithLazyTx(true)) // Driver WITH global lazyTx enabled
		defer func() { _ = db.Close() }()

		require.NoError(t, retry.DoTx(scope.Ctx, db, func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, "SELECT 1")

			return err
		}, retry.WithLazyTx(false)))

		require.False(t, lazyTxDetected.Load(), "lazy transaction should be disabled via retry.WithLazyTx(false)")
	})
}
