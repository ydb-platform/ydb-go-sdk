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
)

type sqlScanner struct {
	v any
}

func (s *sqlScanner) Scan(v any) error {
	s.v = v

	return nil
}

var _ sql.Scanner = (*sqlScanner)(nil)

func TestDatabaseSqlScanner(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var db1, db2 *sql.DB
	{
		nativeDriver, err := ydb.Open(ctx,
			os.Getenv("YDB_CONNECTION_STRING"),
			ydb.WithAccessTokenCredentials(os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")),
		)
		require.NoError(t, err)
		defer func() {
			_ = nativeDriver.Close(ctx)
		}()

		connector, err := ydb.Connector(nativeDriver,
			ydb.WithQueryService(false),
		)
		require.NoError(t, err)

		defer func() {
			_ = connector.Close()
		}()

		db1 = sql.OpenDB(connector)
	}
	{
		nativeDriver, err := ydb.Open(ctx,
			os.Getenv("YDB_CONNECTION_STRING"),
			ydb.WithAccessTokenCredentials(os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")),
		)
		require.NoError(t, err)
		defer func() {
			_ = nativeDriver.Close(ctx)
		}()

		connector, err := ydb.Connector(nativeDriver,
			ydb.WithQueryService(true),
		)
		require.NoError(t, err)

		defer func() {
			_ = connector.Close()
		}()

		db2 = sql.OpenDB(connector)
	}
	for _, ttt := range []struct {
		name string
		sql  string
		scan func(row *sql.Row) (_ error, act, exp any)
	}{
		{
			name: "Uint64",
			sql:  "SELECT CAST(1 AS Uint64)",
			scan: func(row *sql.Row) (_ error, act, exp any) {
				var v uint64
				return row.Scan(&v), v, uint64(1)
			},
		},
		{
			name: "JSON(`{}`)",
			sql:  "SELECT CAST('{}' AS JSON)",
			scan: func(row *sql.Row) (_ error, act, exp any) {
				v := sqlScanner{}
				return row.Scan(&v), v, sqlScanner{v: []byte("{}")}
			},
		},
		{
			name: "JSON(null)",
			sql:  "SELECT CAST(null AS JSON)",
			scan: func(row *sql.Row) (_ error, act, exp any) {
				v := sqlScanner{}
				return row.Scan(&v), v, sqlScanner{v: (any)(nil)}
			},
		},
	} {
		t.Run(ttt.name, func(t *testing.T) {
			for _, tt := range []struct {
				name string
				db   *sql.DB
			}{
				{
					name: "ydb.WithQueryService(false)",
					db:   db1,
				},
				{
					name: "ydb.WithQueryService(true)",
					db:   db2,
				},
			} {
				t.Run(tt.name, func(t *testing.T) {
					row := tt.db.QueryRowContext(ctx, ttt.sql)

					err, act, exp := ttt.scan(row)
					require.NoError(t, err)

					require.Equal(t, exp, act)
				})
			}
		})
	}
}
