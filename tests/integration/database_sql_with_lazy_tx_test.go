//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

func TestDatabaseSqlWithLazyTx(t *testing.T) {
	scope := newScope(t)

	t.Run("WithLazyTxTrue", func(t *testing.T) {
		db := scope.SQLDriverWithFolder(
			ydb.WithTablePathPrefix(scope.Folder()),
			ydb.WithAutoDeclare(),
			ydb.WithQueryService(true),
		)

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
	})

	t.Run("WithLazyTxFalse", func(t *testing.T) {
		db := scope.SQLDriverWithFolder(
			ydb.WithTablePathPrefix(scope.Folder()),
			ydb.WithAutoDeclare(),
			ydb.WithQueryService(true),
		)

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
	})

	t.Run("WithLazyTxOverridesDriverDefault", func(t *testing.T) {
		db := scope.SQLDriverWithFolder(
			ydb.WithTablePathPrefix(scope.Folder()),
			ydb.WithAutoDeclare(),
			ydb.WithQueryService(true),
		)

		require.NoError(t, retry.DoTx(scope.Ctx, db, func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, "SELECT 1")

			return err
		}, retry.WithLazyTx(true)))
	})
}
