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

// TestDatabaseSqlNilArgIsNullType verifies that passing nil as a database/sql
// argument results in NullType (not Void) so that YDB accepts it for optional
// columns.
func TestDatabaseSqlNilArgIsNullType(t *testing.T) {
	var (
		scope = newScope(t)
		db    = scope.SQLDriverWithFolder(
			ydb.WithAutoDeclare(),
		)
	)

	tableName := scope.TableName(withCreateTableQueryTemplate(`
		PRAGMA TablePathPrefix("{{.TablePathPrefix}}");
		CREATE TABLE {{.TableName}} (
			id Int64 NOT NULL,
			val Text,
			PRIMARY KEY (id)
		)
	`))

	// Insert a row where the optional column is explicitly nil.
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
	require.NoError(t, err)

	// Read back and verify that the value is NULL.
	var val sql.NullString
	row := db.QueryRowContext(scope.Ctx,
		"SELECT val FROM `"+tableName+"` WHERE id = $id",
		sql.Named("id", int64(1)),
	)
	require.NoError(t, row.Scan(&val))
	require.False(t, val.Valid, "expected NULL value for optional column")
}
