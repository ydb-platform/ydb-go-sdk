package ydb_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
)

//nolint:maintidx
func TestQueryBind(t *testing.T) {
	now := time.Now()
	for _, tt := range []struct {
		b      testutil.QueryBindings
		sql    string
		args   []interface{}
		yql    string
		params *table.QueryParameters
		err    error
	}{
		{
			b: testutil.QueryBind(
				ydb.WithTablePathPrefix("/local/test"),
				ydb.WithAutoDeclare(),
				ydb.WithPositionalArgs(),
			),
			sql: `$cnt = (SELECT 2 * COUNT(*) FROM my_table);

UPDATE my_table SET data = CAST($cnt AS Optional<Uint64>) WHERE id = ?;`,
			args: []interface{}{uint64(6)},
			yql: `-- bind TablePathPrefix
PRAGMA TablePathPrefix("/local/test");

-- bind declares
DECLARE $p0 AS Uint64;

-- origin query with positional args replacement
$cnt = (SELECT 2 * COUNT(*) FROM my_table);

UPDATE my_table SET data = CAST($cnt AS Optional<Uint64>) WHERE id = $p0;`,
			params: table.NewQueryParameters(
				table.ValueParam("$p0", types.Uint64Value(6)),
			),
		},
		{
			b: testutil.QueryBind(
				ydb.WithTablePathPrefix("/local/test"),
				ydb.WithAutoDeclare(),
				ydb.WithNumericArgs(),
			),
			sql: `$cnt = (SELECT 2 * COUNT(*) FROM my_table);

UPDATE my_table SET data = CAST($cnt AS Uint64) WHERE id = $1;`,
			args: []interface{}{uint64(6)},
			yql: `-- bind TablePathPrefix
PRAGMA TablePathPrefix("/local/test");

-- bind declares
DECLARE $p0 AS Uint64;

-- origin query with numeric args replacement
$cnt = (SELECT 2 * COUNT(*) FROM my_table);

UPDATE my_table SET data = CAST($cnt AS Uint64) WHERE id = $p0;`,
			params: table.NewQueryParameters(
				table.ValueParam("$p0", types.Uint64Value(6)),
			),
		},
		{
			b:      nil,
			sql:    "SELECT ?, $1, $p0",
			yql:    "SELECT ?, $1, $p0",
			params: table.NewQueryParameters(),
		},
		{
			b:      testutil.QueryBind(),
			sql:    "SELECT ?, $1, $p0",
			yql:    "SELECT ?, $1, $p0",
			params: table.NewQueryParameters(),
		},
		{
			b: testutil.QueryBind(
				ydb.WithTablePathPrefix("/local/path/to/table"),
			),
			sql: "SELECT ?, $1, $p0",
			yql: `-- bind TablePathPrefix
PRAGMA TablePathPrefix("/local/path/to/table");

SELECT ?, $1, $p0`,
			params: table.NewQueryParameters(),
		},
		{
			b: testutil.QueryBind(
				ydb.WithAutoDeclare(),
			),
			sql: "SELECT $p0, $p1, $p2, $p0, $p1",
			args: []interface{}{
				1,
				"test",
				[]string{
					"test1",
					"test2",
					"test3",
				},
			},
			yql: `-- bind declares
DECLARE $p0 AS Int32;
DECLARE $p1 AS Utf8;
DECLARE $p2 AS List<Utf8>;

SELECT $p0, $p1, $p2, $p0, $p1`,
			params: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(1)),
				table.ValueParam("$p1", types.TextValue("test")),
				table.ValueParam("$p2", types.ListValue(
					types.TextValue("test1"),
					types.TextValue("test2"),
					types.TextValue("test3"),
				)),
			),
		},
		{
			b: testutil.QueryBind(
				ydb.WithAutoDeclare(),
				ydb.WithPositionalArgs(),
			),
			sql: "SELECT ?, ?, ?",
			args: []interface{}{
				1,
				"test",
				[]string{
					"test1",
					"test2",
					"test3",
				},
			},
			yql: `-- bind declares
DECLARE $p0 AS Int32;
DECLARE $p1 AS Utf8;
DECLARE $p2 AS List<Utf8>;

-- origin query with positional args replacement
SELECT $p0, $p1, $p2`,
			params: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(1)),
				table.ValueParam("$p1", types.TextValue("test")),
				table.ValueParam("$p2", types.ListValue(
					types.TextValue("test1"),
					types.TextValue("test2"),
					types.TextValue("test3"),
				)),
			),
		},
		{
			b: testutil.QueryBind(
				ydb.WithAutoDeclare(),
				ydb.WithNumericArgs(),
			),
			sql: "SELECT $1, $2, $3, $1, $2",
			args: []interface{}{
				1,
				"test",
				[]string{
					"test1",
					"test2",
					"test3",
				},
			},
			yql: `-- bind declares
DECLARE $p0 AS Int32;
DECLARE $p1 AS Utf8;
DECLARE $p2 AS List<Utf8>;

-- origin query with numeric args replacement
SELECT $p0, $p1, $p2, $p0, $p1`,
			params: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(1)),
				table.ValueParam("$p1", types.TextValue("test")),
				table.ValueParam("$p2", types.ListValue(
					types.TextValue("test1"),
					types.TextValue("test2"),
					types.TextValue("test3"),
				)),
			),
		},
		{
			b: testutil.QueryBind(
				ydb.WithTablePathPrefix("/local/path/to/my/folder"),
				ydb.WithAutoDeclare(),
				ydb.WithPositionalArgs(),
			),
			sql: "SELECT a, b, c WHERE id = ? AND date < ? AND value IN (?)",
			args: []interface{}{
				1, now, []string{"3"},
			},
			yql: `-- bind TablePathPrefix
PRAGMA TablePathPrefix("/local/path/to/my/folder");

-- bind declares
DECLARE $p0 AS Int32;
DECLARE $p1 AS Timestamp;
DECLARE $p2 AS List<Utf8>;

-- origin query with positional args replacement
SELECT a, b, c WHERE id = $p0 AND date < $p1 AND value IN ($p2)`,
			params: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(1)),
				table.ValueParam("$p1", types.TimestampValueFromTime(now)),
				table.ValueParam("$p2", types.ListValue(types.TextValue("3"))),
			),
		},
		{
			b: testutil.QueryBind(
				ydb.WithTablePathPrefix("/local/"),
				ydb.WithAutoDeclare(),
				ydb.WithPositionalArgs(),
				ydb.WithNumericArgs(),
			),
			sql: `SELECT 1`,
			yql: `-- bind TablePathPrefix
PRAGMA TablePathPrefix("/local/");

SELECT 1`,
			params: table.NewQueryParameters(),
		},
		{
			b: testutil.QueryBind(
				ydb.WithTablePathPrefix("/local/"),
				ydb.WithAutoDeclare(),
			),
			sql: `
DECLARE $param1 AS Text; -- some comment
DECLARE $param2 AS Text;
SELECT $param1, $param2`,
			args: []interface{}{
				sql.Named("param1", 100),
				sql.Named("$param2", 200),
			},
			yql: `-- bind TablePathPrefix
PRAGMA TablePathPrefix("/local/");

-- bind declares
DECLARE $param1 AS Int32;
DECLARE $param2 AS Int32;


DECLARE $param1 AS Text; -- some comment
DECLARE $param2 AS Text;
SELECT $param1, $param2`,
			params: table.NewQueryParameters(
				table.ValueParam("$param1", types.Int32Value(100)),
				table.ValueParam("$param2", types.Int32Value(200)),
			),
		},
		{
			b: testutil.QueryBind(
				ydb.WithTablePathPrefix("/local/"),
				ydb.WithAutoDeclare(),
			),
			sql: `
DECLARE $param2 AS Text; -- some comment
SELECT $param1, $param2`,
			args: []interface{}{
				sql.Named("param1", 100),
				sql.Named("$param2", 200),
			},
			yql: `-- bind TablePathPrefix
PRAGMA TablePathPrefix("/local/");

-- bind declares
DECLARE $param1 AS Int32;
DECLARE $param2 AS Int32;


DECLARE $param2 AS Text; -- some comment
SELECT $param1, $param2`,
			params: table.NewQueryParameters(
				table.ValueParam("$param1", types.Int32Value(100)),
				table.ValueParam("$param2", types.Int32Value(200)),
			),
		},
		{
			sql:    "SELECT 1",
			yql:    "SELECT 1",
			params: table.NewQueryParameters(),
		},
		{
			sql: `
SELECT 1`,
			yql: `
SELECT 1`,
			params: table.NewQueryParameters(),
		},
		{
			b:   testutil.QueryBind(ydb.WithPositionalArgs()),
			sql: "SELECT ?, ?",
			args: []interface{}{
				1,
			},
			err: bind.ErrInconsistentArgs,
		},
		{
			b:   testutil.QueryBind(ydb.WithNumericArgs()),
			sql: "SELECT $0, $1",
			args: []interface{}{
				1, 1,
			},
			err: bind.ErrUnexpectedNumericArgZero,
		},
		{
			b:   testutil.QueryBind(ydb.WithPositionalArgs()),
			sql: "SELECT ?, ? -- some comment",
			args: []interface{}{
				100,
				200,
			},
			yql: `-- origin query with positional args replacement
SELECT $p0, $p1 -- some comment`,
			params: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			),
		},
		{
			b:   testutil.QueryBind(ydb.WithPositionalArgs()),
			sql: "SELECT ?, ? -- some comment",
			args: []interface{}{
				100,
			},
			yql: `-- origin query with positional args replacement
SELECT $p0, $p1 -- some comment`,
			params: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(100)),
			),
			err: bind.ErrInconsistentArgs,
		},
		{
			b:   testutil.QueryBind(ydb.WithPositionalArgs()),
			sql: "SELECT ?, ?, ?",
			args: []interface{}{
				100,
				200,
			},
			yql: `-- origin query with positional args replacement
SELECT $p0, $p1`,
			params: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			),
			err: bind.ErrInconsistentArgs,
		},
		{
			b: testutil.QueryBind(ydb.WithPositionalArgs()),
			sql: `
SELECT ? /* some comment with ? */, ?`,
			args: []interface{}{
				100,
				200,
			},
			yql: `-- origin query with positional args replacement

SELECT $p0 /* some comment with ? */, $p1`,
			params: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			),
		},
		{
			b: testutil.QueryBind(
				ydb.WithTablePathPrefix("/local/"),
				ydb.WithPositionalArgs(),
			),
			sql: "SELECT ?, ?",
			args: []interface{}{
				100,
				200,
			},
			yql: `-- bind TablePathPrefix
PRAGMA TablePathPrefix("/local/");

-- origin query with positional args replacement
SELECT $p0, $p1`,
			params: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			),
		},
		{
			b: testutil.QueryBind(
				ydb.WithTablePathPrefix("/local/"),
				ydb.WithAutoDeclare(),
				ydb.WithPositionalArgs(),
			),
			sql: "SELECT ?, ?",
			args: []interface{}{
				100,
				200,
			},
			yql: `-- bind TablePathPrefix
PRAGMA TablePathPrefix("/local/");

-- bind declares
DECLARE $p0 AS Int32;
DECLARE $p1 AS Int32;

-- origin query with positional args replacement
SELECT $p0, $p1`,
			params: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			),
		},
		{
			b:   testutil.QueryBind(ydb.WithNumericArgs()),
			sql: "SELECT $1 /* some comment with $3 */, $2",
			args: []interface{}{
				1,
			},
			err: bind.ErrInconsistentArgs,
		},
		{
			b:   testutil.QueryBind(ydb.WithNumericArgs()),
			sql: "SELECT $1 /* some comment with $3 */, $2",
			args: []interface{}{
				100,
				200,
			},
			yql: `-- origin query with numeric args replacement
SELECT $p0 /* some comment with $3 */, $p1`,
			params: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			),
		},
		{
			b: testutil.QueryBind(
				ydb.WithAutoDeclare(),
				ydb.WithNumericArgs(),
			),
			sql: "SELECT $1, $2",
			args: []interface{}{
				100,
				200,
			},
			yql: `-- bind declares
DECLARE $p0 AS Int32;
DECLARE $p1 AS Int32;

-- origin query with numeric args replacement
SELECT $p0, $p1`,
			params: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			),
		},
		{
			b: testutil.QueryBind(ydb.WithNumericArgs()),
			sql: `
SELECT $1, $2`,
			args: []interface{}{
				100,
				200,
			},
			yql: `-- origin query with numeric args replacement

SELECT $p0, $p1`,
			params: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			),
		},
		{
			b: testutil.QueryBind(
				ydb.WithTablePathPrefix("/local/"),
				ydb.WithNumericArgs(),
			),
			sql: "SELECT $1 /* some comment with $3 */, $2",
			args: []interface{}{
				100,
				200,
			},
			yql: `-- bind TablePathPrefix
PRAGMA TablePathPrefix("/local/");

-- origin query with numeric args replacement
SELECT $p0 /* some comment with $3 */, $p1`,
			params: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			),
		},
		{
			b: testutil.QueryBind(
				ydb.WithTablePathPrefix("/local/"),
				ydb.WithAutoDeclare(),
				ydb.WithNumericArgs(),
			),
			sql: "SELECT $1, $2",
			args: []interface{}{
				100,
				200,
			},
			yql: `-- bind TablePathPrefix
PRAGMA TablePathPrefix("/local/");

-- bind declares
DECLARE $p0 AS Int32;
DECLARE $p1 AS Int32;

-- origin query with numeric args replacement
SELECT $p0, $p1`,
			params: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			),
		},
		{
			b:   testutil.QueryBind(ydb.WithTablePathPrefix("/local/")),
			sql: "SELECT 1",
			yql: `-- bind TablePathPrefix
PRAGMA TablePathPrefix("/local/");

SELECT 1`,
			params: table.NewQueryParameters(),
		},
		{
			b: testutil.QueryBind(
				ydb.WithTablePathPrefix("/local/"),
				ydb.WithAutoDeclare(),
			),
			sql: "SELECT 1",
			yql: `-- bind TablePathPrefix
PRAGMA TablePathPrefix("/local/");

SELECT 1`,
			params: table.NewQueryParameters(),
		},
		{
			b: testutil.QueryBind(
				ydb.WithTablePathPrefix("/local/"),
				ydb.WithAutoDeclare(),
			),
			sql: "SELECT $param1, $param2",
			args: []interface{}{
				sql.Named("param1", 100),
				sql.Named("$param2", 200),
			},
			yql: `-- bind TablePathPrefix
PRAGMA TablePathPrefix("/local/");

-- bind declares
DECLARE $param1 AS Int32;
DECLARE $param2 AS Int32;

SELECT $param1, $param2`,
			params: table.NewQueryParameters(
				table.ValueParam("$param1", types.Int32Value(100)),
				table.ValueParam("$param2", types.Int32Value(200)),
			),
		},
		{
			b: testutil.QueryBind(
				ydb.WithAutoDeclare(),
				ydb.WithPositionalArgs(),
			),
			sql:  `SELECT ?;`,
			args: []interface{}{time.Unix(123, 456)},
			yql: `-- bind declares
DECLARE $p0 AS Timestamp;

-- origin query with positional args replacement
SELECT $p0;`,
			params: ydb.ParamsBuilder().Param("$p0").Timestamp(time.Unix(123, 456)).Build(),
		},
		{
			b: testutil.QueryBind(
				ydb.WithAutoDeclare(),
				ydb.WithPositionalArgs(),
				ydb.WithWideTimeTypes(true),
			),
			sql:  `SELECT ?;`,
			args: []interface{}{time.Unix(123, 456)},
			yql: `-- bind declares
DECLARE $p0 AS Timestamp64;

-- origin query with positional args replacement
SELECT $p0;`,
			params: ydb.ParamsBuilder().Param("$p0").Timestamp64(time.Unix(123, 456)).Build(),
		},
	} {
		t.Run("", func(t *testing.T) {
			yql, parameters, err := tt.b.ToYdb(tt.sql, tt.args...)
			if tt.err != nil {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.yql, yql)
				require.Equal(t, *tt.params, parameters)
			}
		})
	}
}
