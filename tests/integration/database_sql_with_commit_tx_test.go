//go:build integration
// +build integration

package integration

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

func TestDatabaseSqlWithCommitTxContext(t *testing.T) {
	scope := newScope(t)

	t.Run("ExecWithCommitTxContext", func(t *testing.T) {
		db := scope.SQLDriverWithFolder(
			ydb.WithTablePathPrefix(scope.Folder()),
			ydb.WithAutoDeclare(),
			ydb.WithQueryService(true),
		)

		ctx := scope.Ctx

		// Start transaction
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)

		// Execute with commit flag - transaction should be committed after this
		_, err = tx.ExecContext(ydb.WithCommitTxContext(ctx), "SELECT 1")
		require.NoError(t, err)

		// Commit should be a no-op (already committed via WithCommitTxContext)
		err = tx.Commit()
		require.NoError(t, err)
	})

	t.Run("ExecWithCommitTxContextThenCommit", func(t *testing.T) {
		db := scope.SQLDriverWithFolder(
			ydb.WithTablePathPrefix(scope.Folder()),
			ydb.WithAutoDeclare(),
			ydb.WithQueryService(true),
		)

		ctx := scope.Ctx

		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)

		// First exec without commit
		_, err = tx.ExecContext(ctx, "SELECT 1")
		require.NoError(t, err)

		// Second exec with commit flag
		_, err = tx.ExecContext(ydb.WithCommitTxContext(ctx), "SELECT 2")
		require.NoError(t, err)

		// Commit should be a no-op (already committed)
		err = tx.Commit()
		require.NoError(t, err)
	})

	t.Run("ExecWithCommitTxContextThenRollback", func(t *testing.T) {
		db := scope.SQLDriverWithFolder(
			ydb.WithTablePathPrefix(scope.Folder()),
			ydb.WithAutoDeclare(),
			ydb.WithQueryService(true),
		)

		ctx := scope.Ctx

		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)

		// Execute with commit flag - transaction is committed
		_, err = tx.ExecContext(ydb.WithCommitTxContext(ctx), "SELECT 1")
		require.NoError(t, err)

		// Rollback should be a no-op (transaction already completed)
		err = tx.Rollback()
		require.NoError(t, err)
	})

	t.Run("QueryWithCommitTxContext", func(t *testing.T) {
		db := scope.SQLDriverWithFolder(
			ydb.WithTablePathPrefix(scope.Folder()),
			ydb.WithAutoDeclare(),
			ydb.WithQueryService(true),
		)

		ctx := scope.Ctx

		// Start transaction
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)

		// Query with commit flag
		rows, err := tx.QueryContext(ydb.WithCommitTxContext(ctx), "SELECT 1")
		require.NoError(t, err)

		for rows.Next() {
			var v int
			require.NoError(t, rows.Scan(&v))
			require.Equal(t, 1, v)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())

		// Commit should be a no-op
		err = tx.Commit()
		require.NoError(t, err)
	})

	t.Run("QueryWithCommitTxContextThenRollback", func(t *testing.T) {
		db := scope.SQLDriverWithFolder(
			ydb.WithTablePathPrefix(scope.Folder()),
			ydb.WithAutoDeclare(),
			ydb.WithQueryService(true),
		)

		ctx := scope.Ctx

		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)

		// Query with commit flag
		rows, err := tx.QueryContext(ydb.WithCommitTxContext(ctx), "SELECT 1")
		require.NoError(t, err)

		for rows.Next() {
			var v int
			require.NoError(t, rows.Scan(&v))
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())

		// Rollback should be a no-op (transaction already completed)
		err = tx.Rollback()
		require.NoError(t, err)
	})
}
