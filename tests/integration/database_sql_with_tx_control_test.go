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
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/common"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
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
				common.WithTxControlHook(ctx, func(txControl *table.TransactionControl) {
					hookCalled = true
					require.Equal(t, table.SerializableReadWriteTxControl().Desc(), txControl.Desc())
				}),
				table.SerializableReadWriteTxControl(),
			),
			db, func(ctx context.Context, cc *sql.Conn) error {
				rows, err := db.QueryContext(ctx, "SELECT 1")
				defer rows.Close()

				return err
			},
		))
		require.True(t, hookCalled || overQueryService)
	})

	t.Run("SerializableReadWriteTxControl", func(t *testing.T) {
		var hookCalled bool
		require.NoError(t, retry.Do(
			ydb.WithTxControl(
				common.WithTxControlHook(ctx, func(txControl *table.TransactionControl) {
					hookCalled = true
					require.Equal(t, table.SerializableReadWriteTxControl().Desc(), txControl.Desc())
				}),
				table.SerializableReadWriteTxControl(),
			),
			db, func(ctx context.Context, cc *sql.Conn) error {
				rows, err := db.QueryContext(ctx, "SELECT 1")
				defer rows.Close()

				return err
			},
		))
		require.True(t, hookCalled || overQueryService)
	})

	t.Run("SnapshotReadOnlyTxControl", func(t *testing.T) {
		var hookCalled bool
		require.NoError(t, retry.Do(
			ydb.WithTxControl(
				common.WithTxControlHook(ctx, func(txControl *table.TransactionControl) {
					hookCalled = true
					require.Equal(t, table.SnapshotReadOnlyTxControl().Desc(), txControl.Desc())
				}),
				table.SnapshotReadOnlyTxControl(),
			),
			db, func(ctx context.Context, cc *sql.Conn) error {
				rows, err := db.QueryContext(ctx, "SELECT 1")
				defer rows.Close()

				return err
			},
		))
		require.True(t, hookCalled || overQueryService)
	})

	t.Run("StaleReadOnlyTxControl", func(t *testing.T) {
		var hookCalled bool
		require.NoError(t, retry.Do(
			ydb.WithTxControl(
				common.WithTxControlHook(ctx, func(txControl *table.TransactionControl) {
					hookCalled = true
					require.Equal(t, table.StaleReadOnlyTxControl().Desc(), txControl.Desc())
				}),
				table.StaleReadOnlyTxControl(),
			),
			db, func(ctx context.Context, cc *sql.Conn) error {
				rows, err := db.QueryContext(ctx, "SELECT 1")
				defer rows.Close()

				return err
			},
		))
		require.True(t, hookCalled || overQueryService)
	})

	t.Run("OnlineReadOnlyTxControl{AllowInconsistentReads:false}", func(t *testing.T) {
		var hookCalled bool
		require.NoError(t, retry.Do(
			ydb.WithTxControl(
				common.WithTxControlHook(ctx, func(txControl *table.TransactionControl) {
					hookCalled = true
					require.Equal(t, table.OnlineReadOnlyTxControl().Desc(), txControl.Desc())
				}),
				table.OnlineReadOnlyTxControl(),
			),
			db, func(ctx context.Context, cc *sql.Conn) error {
				rows, err := db.QueryContext(ctx, "SELECT 1")
				defer rows.Close()

				return err
			},
		))
		require.True(t, hookCalled || overQueryService)
	})

	t.Run("OnlineReadOnlyTxControl{AllowInconsistentReads:true})", func(t *testing.T) {
		var hookCalled bool
		require.NoError(t, retry.Do(
			ydb.WithTxControl(
				common.WithTxControlHook(ctx, func(txControl *table.TransactionControl) {
					hookCalled = true
					require.Equal(t, table.OnlineReadOnlyTxControl(table.WithInconsistentReads()).Desc(), txControl.Desc())
				}),
				table.OnlineReadOnlyTxControl(table.WithInconsistentReads()),
			),
			db, func(ctx context.Context, cc *sql.Conn) error {
				rows, err := db.QueryContext(ctx, "SELECT 1")
				defer rows.Close()

				return err
			},
		))
		require.True(t, hookCalled || overQueryService)
	})
}
