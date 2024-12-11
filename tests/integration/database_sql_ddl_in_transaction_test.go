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

func TestDatabaseSqlDDLInTransaction(t *testing.T) {
	var (
		scope = newScope(t)
		db    = scope.SQLDriverWithFolder()
	)

	defer func() {
		_ = db.Close()
	}()

	f := func(ctx context.Context, tx *sql.Tx) (err error) {
		_, err = tx.ExecContext(
			ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
			`DROP TABLE IF EXISTS test`,
		)
		if err != nil {
			return err
		}

		_, err = tx.ExecContext(ctx, `CREATE TABLE test (id Uint64, PRIMARY KEY (id))`)
		if err != nil {
			return err
		}

		return err
	}

	t.Run("InTransaction", func(t *testing.T) {
		t.Run("WrongQueryMode", func(t *testing.T) {
			err := retry.DoTx(scope.Ctx, db, f,
				retry.WithIdempotent(true), retry.WithTxOptions(&sql.TxOptions{
					Isolation: sql.LevelSnapshot,
					ReadOnly:  true,
				}),
			)
			require.Error(t, err)
		})
		t.Run("SnapshotROIsolation", func(t *testing.T) {
			err := retry.DoTx(scope.Ctx, db, f,
				retry.WithIdempotent(true), retry.WithTxOptions(&sql.TxOptions{
					Isolation: sql.LevelSnapshot,
					ReadOnly:  true,
				}),
			)
			require.Error(t, err)
		})
		t.Run("SerializableRWIsolation", func(t *testing.T) {
			err := retry.DoTx(scope.Ctx, db,
				f, retry.WithIdempotent(true), retry.WithTxOptions(&sql.TxOptions{
					Isolation: sql.LevelSerializable,
					ReadOnly:  false,
				}),
			)
			require.Error(t, err)
		})
		t.Run("FakeTx", func(t *testing.T) {
			connector, err := ydb.Connector(scope.Driver(),
				ydb.WithTablePathPrefix(scope.Folder()),
				ydb.WithFakeTx(
					ydb.SchemeQueryMode,
					ydb.QueryExecuteQueryMode,
				),
			)
			require.NoError(t, err)

			db := sql.OpenDB(connector)

			err = db.PingContext(scope.Ctx)
			require.NoError(t, err)

			err = retry.DoTx(ydb.WithQueryMode(scope.Ctx, ydb.SchemeQueryMode), db,
				f, retry.WithIdempotent(true), retry.WithTxOptions(&sql.TxOptions{
					Isolation: sql.LevelSerializable,
					ReadOnly:  false,
				}),
			)
			require.NoError(t, err)
		})
	})
}
