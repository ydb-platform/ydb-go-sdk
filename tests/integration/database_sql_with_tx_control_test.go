//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

func TestDatabaseSqlWithTxControl(t *testing.T) {
	var (
		ctx   = xtest.Context(t)
		scope = newScope(t)
		db    = scope.SQLDriverWithFolder(
			ydb.WithTablePathPrefix(scope.Folder()),
			ydb.WithAutoDeclare(),
			ydb.WithQueryService(true),
		)
	)
	overQueryService := false

	if v, has := os.LookupEnv("YDB_DATABASE_SQL_OVER_QUERY_SERVICE"); has && v != "" {
		overQueryService = true
	}

	t.Run("default", func(t *testing.T) {
		var hookCalled bool
		require.NoError(t, retry.Do(
			ydb.WithTxControl(
				tx.WithTxControlHook(ctx, func(txControl *tx.Control) {
					hookCalled = true
					require.Equal(t, tx.SerializableReadWriteTxControl(tx.CommitTx()), txControl)
				}),
				tx.SerializableReadWriteTxControl(tx.CommitTx()),
			),
			db, func(ctx context.Context, cc *sql.Conn) error {
				_, err := db.QueryContext(ctx, "SELECT 1")
				return err
			},
		))
		require.True(t, hookCalled || overQueryService)
	})

	t.Run("SerializableReadWriteTxControl", func(t *testing.T) {
		var hookCalled bool
		require.NoError(t, retry.Do(
			ydb.WithTxControl(
				tx.WithTxControlHook(ctx, func(txControl *tx.Control) {
					hookCalled = true
					require.Equal(t, tx.SerializableReadWriteTxControl(tx.CommitTx()), txControl)
				}),
				tx.SerializableReadWriteTxControl(tx.CommitTx()),
			),
			db, func(ctx context.Context, cc *sql.Conn) error {
				_, err := db.QueryContext(ctx, "SELECT 1")
				return err
			},
		))
		require.True(t, hookCalled || overQueryService)
	})

	t.Run("SnapshotReadOnlyTxControl", func(t *testing.T) {
		var hookCalled bool
		require.NoError(t, retry.Do(
			ydb.WithTxControl(
				tx.WithTxControlHook(ctx, func(txControl *tx.Control) {
					hookCalled = true
					require.Equal(t, tx.SnapshotReadOnlyTxControl(), txControl)
				}),
				tx.SnapshotReadOnlyTxControl(),
			),
			db, func(ctx context.Context, cc *sql.Conn) error {
				_, err := db.QueryContext(ctx, "SELECT 1")
				return err
			},
		))
		require.True(t, hookCalled || overQueryService)
	})

	t.Run("StaleReadOnlyTxControl", func(t *testing.T) {
		var hookCalled bool
		require.NoError(t, retry.Do(
			ydb.WithTxControl(
				tx.WithTxControlHook(ctx, func(txControl *tx.Control) {
					hookCalled = true
					require.Equal(t, tx.StaleReadOnlyTxControl(), txControl)
				}),
				tx.StaleReadOnlyTxControl(),
			),
			db, func(ctx context.Context, cc *sql.Conn) error {
				_, err := db.QueryContext(ctx, "SELECT 1")
				return err
			},
		))
		require.True(t, hookCalled || overQueryService)
	})

	t.Run("OnlineReadOnlyTxControl{AllowInconsistentReads:false}", func(t *testing.T) {
		var hookCalled bool
		require.NoError(t, retry.Do(
			ydb.WithTxControl(
				tx.WithTxControlHook(ctx, func(txControl *tx.Control) {
					hookCalled = true
					require.Equal(t, tx.OnlineReadOnlyTxControl(), txControl)
				}),
				tx.OnlineReadOnlyTxControl(),
			),
			db, func(ctx context.Context, cc *sql.Conn) error {
				_, err := db.QueryContext(ctx, "SELECT 1")
				return err
			},
		))
		require.True(t, hookCalled || overQueryService)
	})

	t.Run("OnlineReadOnlyTxControl{AllowInconsistentReads:true})", func(t *testing.T) {
		var hookCalled bool
		require.NoError(t, retry.Do(
			ydb.WithTxControl(
				tx.WithTxControlHook(ctx, func(txControl *tx.Control) {
					hookCalled = true
					require.Equal(t, tx.OnlineReadOnlyTxControl(tx.WithInconsistentReads()), txControl)
				}),
				tx.OnlineReadOnlyTxControl(tx.WithInconsistentReads()),
			),
			db, func(ctx context.Context, cc *sql.Conn) error {
				_, err := db.QueryContext(ctx, "SELECT 1")
				return err
			},
		))
		require.True(t, hookCalled || overQueryService)
	})

	t.Run("MatchingTxControlInTransaction", func(t *testing.T) {
		// This test verifies that when using retry.DoTx with a transaction,
		// passing a matching TxControl through context should NOT throw an error
		err := retry.DoTx(ctx, db, func(ctx context.Context, sqlTx *sql.Tx) error {
			// Execute a query with the same TxControl as the transaction was started with
			// This should succeed after the fix
			_, err := sqlTx.QueryContext(
				ydb.WithTxControl(ctx, tx.SerializableReadWriteTxControl()),
				"SELECT 1",
			)
			return err
		}, retry.WithIdempotent(true), retry.WithTxOptions(&sql.TxOptions{
			Isolation: sql.LevelSerializable,
			ReadOnly:  false,
		}))
		require.NoError(t, err)
	})

	t.Run("DifferentTxControlInTransaction", func(t *testing.T) {
		// This test verifies that when using retry.DoTx with a transaction,
		// passing a different TxControl through context SHOULD throw an error
		err := retry.DoTx(ctx, db, func(ctx context.Context, sqlTx *sql.Tx) error {
			// Execute a query with a different TxControl than the transaction was started with
			// This should fail because the TxControl doesn't match
			_, err := sqlTx.QueryContext(
				ydb.WithTxControl(ctx, tx.SnapshotReadOnlyTxControl()),
				"SELECT 1",
			)
			return err
		}, retry.WithIdempotent(true), retry.WithTxOptions(&sql.TxOptions{
			Isolation: sql.LevelSerializable,
			ReadOnly:  false,
		}))
		require.Error(t, err)
		require.ErrorIs(t, err, query.ErrOptionNotForTxExecute)
	})
}
