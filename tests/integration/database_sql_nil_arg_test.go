//go:build integration

package integration

import (
	"context"
	"database/sql"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

func TestDatabaseSqlNilArgIsNullType(t *testing.T) {
	var (
		scope = newScope(t)
		db    = scope.SQLDriverWithFolder(
			ydb.WithAutoDeclare(),
		)
	)

	tableName := scope.TableName()

	err := retry.Do(scope.Ctx, db,
		func(ctx context.Context, cc *sql.Conn) error {
			_, err := cc.ExecContext(ctx,
				"UPSERT INTO `"+tableName+"` (id, val) VALUES ($id, $val)",
				sql.Named("id", int64(1)),
				sql.Named("val", nil),
			)

			return err
		},
		retry.WithIdempotent(true),
	)
	scope.Require.NoError(err)

	var val sql.NullString
	row := db.QueryRowContext(scope.Ctx,
		"SELECT val FROM `"+tableName+"` WHERE id = $id",
		sql.Named("id", int64(1)),
	)
	scope.Require.NoError(row.Scan(&val))
	scope.Require.False(val.Valid)
}
