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
	for _, test := range []struct {
		name         string
		queryService bool
		positional   bool
	}{
		{"query/named", true, false},
		{"table/named", false, false},
		{"query/positional", true, true},
		{"table/positional", false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			scope := newScope(t)
			opts := []ydb.ConnectorOption{ydb.WithAutoDeclare(), ydb.WithQueryService(test.queryService)}
			if test.positional {
				opts = append(opts, ydb.WithPositionalArgs())
			}
			db := scope.SQLDriverWithFolder(opts...)

			tableName := scope.TableName()

			writeSQL := "UPSERT INTO `" + tableName + "` (id, val) VALUES ($id, $val)"
			readSQL := "SELECT val FROM `" + tableName + "` WHERE id = $id"
			writeArgs := []any{sql.Named("id", int64(1)), sql.Named("val", nil)}
			readArgs := []any{sql.Named("id", int64(1))}
			if test.positional {
				writeSQL = "UPSERT INTO `" + tableName + "` (id, val) VALUES (?, ?)"
				readSQL = "SELECT val FROM `" + tableName + "` WHERE id = ?"
				writeArgs = []any{int64(1), nil}
				readArgs = []any{int64(1)}
			}
			err := retry.Do(scope.Ctx, db,
				func(ctx context.Context, cc *sql.Conn) error {
					_, err := cc.ExecContext(ctx, writeSQL, writeArgs...)

					return err
				},
				retry.WithIdempotent(true),
			)
			scope.Require.NoError(err)

			var val sql.NullString
			row := db.QueryRowContext(scope.Ctx, readSQL, readArgs...)
			scope.Require.NoError(row.Scan(&val))
			scope.Require.False(val.Valid)
		})
	}
}
