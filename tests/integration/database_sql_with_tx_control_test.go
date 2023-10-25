//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql"
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

	t.Run("default", func(t *testing.T) {
		var hookCalled bool
		require.NoError(t, retry.Do(
			ydb.WithTxControl(
				xsql.WithTxControlHook(ctx, func(txControl *table.TransactionControl) {
					hookCalled = true
					require.Equal(t, table.SerializableReadWriteTxControl().Desc(), txControl.Desc())
				}),
				table.SerializableReadWriteTxControl(),
			),
			db, func(ctx context.Context, cc *sql.Conn) error {
				_, err := db.QueryContext(ctx, "SELECT 1")
				return err
			},
		))
		require.True(t, hookCalled)
	})

	t.Run("SerializableReadWriteTxControl", func(t *testing.T) {
		var hookCalled bool
		require.NoError(t, retry.Do(
			ydb.WithTxControl(
				xsql.WithTxControlHook(ctx, func(txControl *table.TransactionControl) {
					hookCalled = true
					require.Equal(t, table.SerializableReadWriteTxControl().Desc(), txControl.Desc())
				}),
				table.SerializableReadWriteTxControl(),
			),
			db, func(ctx context.Context, cc *sql.Conn) error {
				_, err := db.QueryContext(ctx, "SELECT 1")
				return err
			},
		))
		require.True(t, hookCalled)
	})

	t.Run("SnapshotReadOnlyTxControl", func(t *testing.T) {
		var hookCalled bool
		require.NoError(t, retry.Do(
			ydb.WithTxControl(
				xsql.WithTxControlHook(ctx, func(txControl *table.TransactionControl) {
					hookCalled = true
					require.Equal(t, table.SnapshotReadOnlyTxControl().Desc(), txControl.Desc())
				}),
				table.SnapshotReadOnlyTxControl(),
			),
			db, func(ctx context.Context, cc *sql.Conn) error {
				_, err := db.QueryContext(ctx, "SELECT 1")
				return err
			},
		))
		require.True(t, hookCalled)
	})

	t.Run("StaleReadOnlyTxControl", func(t *testing.T) {
		var hookCalled bool
		require.NoError(t, retry.Do(
			ydb.WithTxControl(
				xsql.WithTxControlHook(ctx, func(txControl *table.TransactionControl) {
					hookCalled = true
					require.Equal(t, table.StaleReadOnlyTxControl().Desc(), txControl.Desc())
				}),
				table.StaleReadOnlyTxControl(),
			),
			db, func(ctx context.Context, cc *sql.Conn) error {
				_, err := db.QueryContext(ctx, "SELECT 1")
				return err
			},
		))
		require.True(t, hookCalled)
	})

	t.Run("OnlineReadOnlyTxControl{AllowInconsistentReads:false}", func(t *testing.T) {
		var hookCalled bool
		require.NoError(t, retry.Do(
			ydb.WithTxControl(
				xsql.WithTxControlHook(ctx, func(txControl *table.TransactionControl) {
					hookCalled = true
					require.Equal(t, table.OnlineReadOnlyTxControl().Desc(), txControl.Desc())
				}),
				table.OnlineReadOnlyTxControl(),
			),
			db, func(ctx context.Context, cc *sql.Conn) error {
				_, err := db.QueryContext(ctx, "SELECT 1")
				return err
			},
		))
		require.True(t, hookCalled)
	})

	t.Run("OnlineReadOnlyTxControl{AllowInconsistentReads:true})", func(t *testing.T) {
		var hookCalled bool
		require.NoError(t, retry.Do(
			ydb.WithTxControl(
				xsql.WithTxControlHook(ctx, func(txControl *table.TransactionControl) {
					hookCalled = true
					require.Equal(t, table.OnlineReadOnlyTxControl(table.WithInconsistentReads()).Desc(), txControl.Desc())
				}),
				table.OnlineReadOnlyTxControl(table.WithInconsistentReads()),
			),
			db, func(ctx context.Context, cc *sql.Conn) error {
				_, err := db.QueryContext(ctx, "SELECT 1")
				return err
			},
		))
		require.True(t, hookCalled)
	})
}
