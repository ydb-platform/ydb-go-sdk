//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
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
	t.Run("write-with-commit", func(t *testing.T) {
		tablePath := scope.TablePath()
		const id = 1
		const value = "committed-value"

		txControl := table.TxControl(
			table.BeginTx(table.WithSerializableReadWrite()),
		)

		err := driver.Table().Do(scope.Ctx, func(ctx context.Context, s table.Session) error {
			tx, res, err := s.Execute(ctx, txControl, "SELECT 1", nil)
			if err != nil {
				return err
			}
			defer res.Close()
			if err = res.Err(); err != nil {
				return err
			}

			query := fmt.Sprintf(
				"UPSERT INTO `%s` (id, val) VALUES (%d, \"%s\")",
				tablePath,
				id,
				value,
			)
			res, err = tx.Execute(ctx, query, nil, options.WithCommit())
			if err != nil {
				return err
			}
			defer res.Close()
			return res.Err()
		})
		scope.Require.NoError(err)

		err = driver.Table().Do(scope.Ctx, func(ctx context.Context, s table.Session) error {
			_, res, err := s.Execute(
				ctx,
				table.DefaultTxControl(),
				fmt.Sprintf("SELECT val FROM `%s` WHERE id = %d", tablePath, id),
				nil,
			)
			if err != nil {
				return err
			}
			defer res.Close()
			if err = res.NextResultSetErr(ctx); err != nil {
				return err
			}
			scope.Require.True(res.NextRow(), "no rows")

			var actual string
			if err = res.ScanNamed(named.Required("val", &actual)); err != nil {
				return err
			}
			scope.Require.Equal(value, actual)
			return res.Err()
		})
		scope.Require.NoError(err)
	})
}
