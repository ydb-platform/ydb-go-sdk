//go:build integration
// +build integration

package integration

import (
	"context"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

func TestTableTxControl(t *testing.T) {
	scope := newScope(t)
	driver := scope.Driver()

	t.Run("rw-auto-commit", func(t *testing.T) {
		txControl := table.TxControl(
			table.BeginTx(table.WithSerializableReadWrite()),
			table.CommitTx(),
		)
		err := driver.Table().Do(scope.Ctx, func(ctx context.Context, s table.Session) error {
			_, _, err := s.Execute(ctx, txControl, "SELECT 1", nil)
			return err
		})
		scope.Require.NoError(err)
	})
	t.Run("online-ro-auto-commit", func(t *testing.T) {
		txControl := table.TxControl(
			table.BeginTx(table.WithOnlineReadOnly(table.WithInconsistentReads())),
			table.CommitTx(),
		)
		err := driver.Table().Do(scope.Ctx, func(ctx context.Context, s table.Session) error {
			_, _, err := s.Execute(ctx, txControl, "SELECT 1", nil)
			return err
		})
		scope.Require.NoError(err)
	})
	t.Run("stale-ro-auto-commit", func(t *testing.T) {
		txControl := table.TxControl(
			table.BeginTx(table.WithStaleReadOnly()),
			table.CommitTx(),
		)
		err := driver.Table().Do(scope.Ctx, func(ctx context.Context, s table.Session) error {
			_, _, err := s.Execute(ctx, txControl, "SELECT 1", nil)
			return err
		})
		scope.Require.NoError(err)
	})
	t.Run("snapshot-ro-auto-commit", func(t *testing.T) {
		txControl := table.TxControl(
			table.BeginTx(table.WithSnapshotReadOnly()),
			table.CommitTx(),
		)
		err := driver.Table().Do(scope.Ctx, func(ctx context.Context, s table.Session) error {
			_, _, err := s.Execute(ctx, txControl, "SELECT 1", nil)
			return err
		})
		scope.Require.NoError(err)
	})
}
