//go:build integration
// +build integration

package integration

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/indexed"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestNoEffectsIfForgetCommitTx(t *testing.T) {
	var (
		ctx          = xtest.Context(t)
		scope        = newScope(t)
		nativeDriver = scope.Driver()
		db           = scope.SQLDriver(ydb.WithTablePathPrefix(scope.Folder()), ydb.WithAutoDeclare(), ydb.WithNumericArgs())
		tablePath    = scope.TablePath() // for auto-create table
	)

	t.Run("native", func(t *testing.T) {
		id := uint64(123)

		// create session
		s, err := nativeDriver.Table().CreateSession(ctx) //nolint:
		require.NoError(t, err)

		// tx1 (without commit)
		tx1, err := s.BeginTransaction(ctx, table.TxSettings(table.WithSerializableReadWrite()))
		require.NoError(t, err)

		// upsert data inside tx1
		_, err = tx1.Execute(ctx, `
			DECLARE $p1 AS Uint64;
			DECLARE $p2 AS Text;
			UPSERT INTO `+"`"+tablePath+"`"+` (
				id, val
			) VALUES (
				$p1, $p2
			);`,
			table.NewQueryParameters(
				table.ValueParam("$p1", types.Uint64Value(id)),
				table.ValueParam("$p2", types.TextValue("1st tx")),
			),
		)
		require.NoError(t, err)

		// check for NO persist data from tx1
		_, result, err := s.Execute(ctx, table.DefaultTxControl(), `
			DECLARE $p1 AS Uint64;
			SELECT val FROM `+"`"+tablePath+"`"+`
			WHERE id = $p1;`,
			table.NewQueryParameters(
				table.ValueParam("$p1", types.Uint64Value(id)),
			),
		)
		require.NoError(t, err)
		require.NoError(t, result.NextResultSetErr(ctx))
		require.False(t, result.NextRow())

		// tx2 (with commit)
		tx2, err := s.BeginTransaction(ctx, table.TxSettings(table.WithSerializableReadWrite()))
		require.NoError(t, err)

		// check for NO data from tx1
		result, err = tx2.Execute(ctx, `
			DECLARE $p1 AS Uint64;
			SELECT val FROM `+"`"+tablePath+"`"+`
			WHERE id = $p1;`,
			table.NewQueryParameters(
				table.ValueParam("$p1", types.Uint64Value(id)),
			),
		)
		require.NoError(t, err)
		require.NoError(t, result.NextResultSetErr(ctx))
		require.False(t, result.NextRow())

		// upsert data inside tx2
		_, err = tx2.Execute(ctx, `
			DECLARE $p1 AS Uint64;
			DECLARE $p2 AS Text;
			UPSERT INTO `+"`"+tablePath+"`"+` (
				id, val
			) VALUES (
				$p1, $p2
			);`,
			table.NewQueryParameters(
				table.ValueParam("$p1", types.Uint64Value(id)),
				table.ValueParam("$p2", types.TextValue("2nd tx")),
			),
		)
		require.NoError(t, err)
		// commit tx2
		_, err = tx2.CommitTx(ctx)
		require.NoError(t, err)

		// check for persist data from tx2
		_, result, err = s.Execute(ctx, table.DefaultTxControl(), `
			DECLARE $p1 AS Uint64;
			SELECT val FROM `+"`"+tablePath+"`"+`
			WHERE id = $p1;`,
			table.NewQueryParameters(
				table.ValueParam("$p1", types.Uint64Value(id)),
			),
		)
		require.NoError(t, err)
		require.NoError(t, result.NextResultSetErr(ctx))
		require.True(t, result.NextRow())

		var value *string
		require.NoError(t, result.Scan(indexed.Optional(&value)))
		require.NoError(t, result.Err())

		require.NotNil(t, value)
		require.Equal(t, "2nd tx", *value)
	})

	t.Run("database/sql", func(t *testing.T) {
		id := uint64(456)

		// create connection === YDB table session
		cc, err := db.Conn(ctx)
		require.NoError(t, err)

		// first tx with no commit
		tx1, err := cc.BeginTx(ctx, &sql.TxOptions{})
		require.NoError(t, err)
		_, err = tx1.ExecContext(ctx, `UPSERT INTO table (id, val) VALUES ($1, $2)`, id, "1st tx")
		require.NoError(t, err)

		// check row for NO write
		var (
			value                  string
			connAlreadyHaveTxError *xsql.ConnAlreadyHaveTxError
		)
		err = db.QueryRowContext(ctx, `SELECT val FROM table WHERE id = $1`, id).Scan(&value)
		require.ErrorIs(t, err, sql.ErrNoRows)

		// second tx on existing conn === session
		_, err = cc.BeginTx(ctx, &sql.TxOptions{})
		require.ErrorAs(t, err, &connAlreadyHaveTxError)
	})
}
