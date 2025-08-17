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
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

func TestDatabaseSqlWithTxControl(t *testing.T) {
	var (
		ctx   = xtest.Context(t)
		scope = newScope(t)
		db    = scope.SQLDriverWithFolder(
			ydb.WithTablePathPrefix(scope.Folder()),
			ydb.WithAutoDeclare(),
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
					require.Equal(t, tx.SerializableReadWriteTxControl(), txControl)
				}),
				tx.SerializableReadWriteTxControl(),
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
					require.Equal(t, tx.SerializableReadWriteTxControl(), txControl)
				}),
				tx.SerializableReadWriteTxControl(),
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
}
