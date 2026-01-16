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

// Derived types for testing Row.Scan
type (
	testInt8    int8
	testInt16   int16
	testInt32   int32
	testInt64   int64
	testUint8   uint8
	testUint16  uint16
	testUint32  uint32
	testUint64  uint64
	testFloat32 float32
	testFloat64 float64
	testBool    bool
	testString  string
	testBytes   []byte
)

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
		{
			name: "Int8",
			sql:  "SELECT CAST(42 AS Int8)",
			scan: func(row *sql.Row) (_ error, act, exp any) {
				var v int8
				return row.Scan(&v), v, int8(42)
			},
		},
		{
			name: "Int8_derived",
			sql:  "SELECT CAST(42 AS Int8)",
			scan: func(row *sql.Row) (_ error, act, exp any) {
				var v testInt8
				return row.Scan(&v), v, testInt8(42)
			},
		},
		{
			name: "Int16",
			sql:  "SELECT CAST(42 AS Int16)",
			scan: func(row *sql.Row) (_ error, act, exp any) {
				var v int16
				return row.Scan(&v), v, int16(42)
			},
		},
		{
			name: "Int16_derived",
			sql:  "SELECT CAST(42 AS Int16)",
			scan: func(row *sql.Row) (_ error, act, exp any) {
				var v testInt16
				return row.Scan(&v), v, testInt16(42)
			},
		},
		{
			name: "Int32",
			sql:  "SELECT CAST(42 AS Int32)",
			scan: func(row *sql.Row) (_ error, act, exp any) {
				var v int32
				return row.Scan(&v), v, int32(42)
			},
		},
		{
			name: "Int32_derived",
			sql:  "SELECT CAST(42 AS Int32)",
			scan: func(row *sql.Row) (_ error, act, exp any) {
				var v testInt32
				return row.Scan(&v), v, testInt32(42)
			},
		},
		{
			name: "Int64",
			sql:  "SELECT CAST(42 AS Int64)",
			scan: func(row *sql.Row) (_ error, act, exp any) {
				var v int64
				return row.Scan(&v), v, int64(42)
			},
		},
		{
			name: "Int64_derived",
			sql:  "SELECT CAST(42 AS Int64)",
			scan: func(row *sql.Row) (_ error, act, exp any) {
				var v testInt64
				return row.Scan(&v), v, testInt64(42)
			},
		},
		{
			name: "Uint8",
			sql:  "SELECT CAST(42 AS Uint8)",
			scan: func(row *sql.Row) (_ error, act, exp any) {
				var v uint8
				return row.Scan(&v), v, uint8(42)
			},
		},
		{
			name: "Uint8_derived",
			sql:  "SELECT CAST(42 AS Uint8)",
			scan: func(row *sql.Row) (_ error, act, exp any) {
				var v testUint8
				return row.Scan(&v), v, testUint8(42)
			},
		},
		{
			name: "Uint16",
			sql:  "SELECT CAST(42 AS Uint16)",
			scan: func(row *sql.Row) (_ error, act, exp any) {
				var v uint16
				return row.Scan(&v), v, uint16(42)
			},
		},
		{
			name: "Uint16_derived",
			sql:  "SELECT CAST(42 AS Uint16)",
			scan: func(row *sql.Row) (_ error, act, exp any) {
				var v testUint16
				return row.Scan(&v), v, testUint16(42)
			},
		},
		{
			name: "Uint32",
			sql:  "SELECT CAST(42 AS Uint32)",
			scan: func(row *sql.Row) (_ error, act, exp any) {
				var v uint32
				return row.Scan(&v), v, uint32(42)
			},
		},
		{
			name: "Uint32_derived",
			sql:  "SELECT CAST(42 AS Uint32)",
			scan: func(row *sql.Row) (_ error, act, exp any) {
				var v testUint32
				return row.Scan(&v), v, testUint32(42)
			},
		},
		{
			name: "Uint64_derived",
			sql:  "SELECT CAST(42 AS Uint64)",
			scan: func(row *sql.Row) (_ error, act, exp any) {
				var v testUint64
				return row.Scan(&v), v, testUint64(42)
			},
		},
		{
			name: "Float32",
			sql:  "SELECT CAST(3.14 AS Float)",
			scan: func(row *sql.Row) (_ error, act, exp any) {
				var v float32
				return row.Scan(&v), v, float32(3.14)
			},
		},
		{
			name: "Float32_derived",
			sql:  "SELECT CAST(3.14 AS Float)",
			scan: func(row *sql.Row) (_ error, act, exp any) {
				var v testFloat32
				return row.Scan(&v), v, testFloat32(3.14)
			},
		},
		{
			name: "Float64",
			sql:  "SELECT CAST(3.14 AS Double)",
			scan: func(row *sql.Row) (_ error, act, exp any) {
				var v float64
				return row.Scan(&v), v, float64(3.14)
			},
		},
		{
			name: "Float64_derived",
			sql:  "SELECT CAST(3.14 AS Double)",
			scan: func(row *sql.Row) (_ error, act, exp any) {
				var v testFloat64
				return row.Scan(&v), v, testFloat64(3.14)
			},
		},
		{
			name: "Bool",
			sql:  "SELECT CAST(true AS Bool)",
			scan: func(row *sql.Row) (_ error, act, exp any) {
				var v bool
				return row.Scan(&v), v, bool(true)
			},
		},
		{
			name: "Bool_derived",
			sql:  "SELECT CAST(true AS Bool)",
			scan: func(row *sql.Row) (_ error, act, exp any) {
				var v testBool
				return row.Scan(&v), v, testBool(true)
			},
		},
		{
			name: "String",
			sql:  "SELECT CAST('hello' AS Utf8)",
			scan: func(row *sql.Row) (_ error, act, exp any) {
				var v string
				return row.Scan(&v), v, string("hello")
			},
		},
		{
			name: "String_derived",
			sql:  "SELECT CAST('hello' AS Utf8)",
			scan: func(row *sql.Row) (_ error, act, exp any) {
				var v testString
				return row.Scan(&v), v, testString("hello")
			},
		},
		{
			name: "Bytes",
			sql:  "SELECT CAST('hello' AS String)",
			scan: func(row *sql.Row) (_ error, act, exp any) {
				var v []byte
				return row.Scan(&v), v, []byte("hello")
			},
		},
		{
			name: "Bytes_derived",
			sql:  "SELECT CAST('hello' AS String)",
			scan: func(row *sql.Row) (_ error, act, exp any) {
				var v testBytes
				return row.Scan(&v), v, testBytes("hello")
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
