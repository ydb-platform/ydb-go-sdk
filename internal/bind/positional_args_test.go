package bind

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestPositionalArgsBindRewriteQuery(t *testing.T) {
	var (
		now = time.Now()
		b   = PositionalArgs{}
	)
	for _, tt := range []struct {
		sql    string
		args   []interface{}
		yql    string
		params []interface{}
		err    error
	}{
		{
			sql: `SELECT ?, ?`,
			args: []interface{}{
				100,
				200,
			},
			yql: `-- origin query with positional args replacement
SELECT $p0, $p1`,
			params: []interface{}{
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			},
			err: nil,
		},
		{
			sql: `SELECT ?, ?`,
			args: []interface{}{
				100,
			},
			yql:    "",
			params: []interface{}{},
			err:    ErrInconsistentArgs,
		},
		{
			sql: `SELECT ?, "?"`,
			args: []interface{}{
				100,
			},
			yql: `-- origin query with positional args replacement
SELECT $p0, "?"`,
			params: []interface{}{
				table.ValueParam("$p0", types.Int32Value(100)),
			},
			err: nil,
		},
		{
			sql: `SELECT ?, '?'`,
			args: []interface{}{
				100,
			},
			yql: `-- origin query with positional args replacement
SELECT $p0, '?'`,
			params: []interface{}{
				table.ValueParam("$p0", types.Int32Value(100)),
			},
			err: nil,
		},
		{
			sql: "SELECT ?, `?`",
			args: []interface{}{
				100,
			},
			yql: "-- origin query with positional args replacement\nSELECT $p0, `?`",
			params: []interface{}{
				table.ValueParam("$p0", types.Int32Value(100)),
			},
			err: nil,
		},
		{
			sql:    "SELECT ?, $1, $p0",
			args:   []interface{}{},
			yql:    "",
			params: []interface{}{},
			err:    ErrInconsistentArgs,
		},
		{
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
			yql: `-- origin query with positional args replacement
SELECT $p0, $p1, $p2`,
			params: []interface{}{
				table.ValueParam("$p0", types.Int32Value(1)),
				table.ValueParam("$p1", types.TextValue("test")),
				table.ValueParam("$p2", types.ListValue(
					types.TextValue("test1"),
					types.TextValue("test2"),
					types.TextValue("test3"),
				)),
			},
			err: nil,
		},
		{
			sql: "SELECT ?, ?, ?",
			args: []interface{}{
				types.Int32Value(1),
				types.TextValue("test"),
				types.ListValue(
					types.TextValue("test1"),
					types.TextValue("test2"),
					types.TextValue("test3"),
				),
			},
			yql: `-- origin query with positional args replacement
SELECT $p0, $p1, $p2`,
			params: []interface{}{
				table.ValueParam("$p0", types.Int32Value(1)),
				table.ValueParam("$p1", types.TextValue("test")),
				table.ValueParam("$p2", types.ListValue(
					types.TextValue("test1"),
					types.TextValue("test2"),
					types.TextValue("test3"),
				)),
			},
			err: nil,
		},
		{
			sql: "SELECT a, b, c WHERE id = ? AND date < ? AND value IN (?)",
			args: []interface{}{
				1, now, []string{"3"},
			},
			yql: `-- origin query with positional args replacement
SELECT a, b, c WHERE id = $p0 AND date < $p1 AND value IN ($p2)`,
			params: []interface{}{
				table.ValueParam("$p0", types.Int32Value(1)),
				table.ValueParam("$p1", types.TimestampValueFromTime(now)),
				table.ValueParam("$p2", types.ListValue(types.TextValue("3"))),
			},
			err: nil,
		},
		{
			sql:    "SELECT 1",
			args:   []interface{}{},
			yql:    "SELECT 1",
			params: []interface{}{},
			err:    nil,
		},
		{
			sql:    `SELECT 1`,
			args:   []interface{}{},
			yql:    `SELECT 1`,
			params: []interface{}{},
			err:    nil,
		},
		{
			sql: "SELECT ?, ?",
			args: []interface{}{
				1,
			},
			yql:    "",
			params: []interface{}{},
			err:    ErrInconsistentArgs,
		},
		{
			sql: "SELECT ?, ? -- some comment with ?",
			args: []interface{}{
				100,
				200,
			},
			yql: `-- origin query with positional args replacement
SELECT $p0, $p1 -- some comment with ?`,
			params: []interface{}{
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			},
			err: nil,
		},
		{
			sql: "SELECT ? /* some comment with ? */, ?",
			args: []interface{}{
				100,
				200,
			},
			yql: `-- origin query with positional args replacement
SELECT $p0 /* some comment with ? */, $p1`,
			params: []interface{}{
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			},
			err: nil,
		},
		{
			sql: "SELECT ?, ? -- some comment with ?",
			args: []interface{}{
				100,
			},
			yql:    "",
			params: []interface{}{},
			err:    ErrInconsistentArgs,
		},
		{
			sql: "SELECT ?, ?, ?",
			args: []interface{}{
				100,
				200,
			},
			yql:    "",
			params: []interface{}{},
			err:    ErrInconsistentArgs,
		},
		{
			sql: `
SELECT ? /* some comment with ? */, ?`,
			args: []interface{}{
				100,
				200,
			},
			yql: `-- origin query with positional args replacement

SELECT $p0 /* some comment with ? */, $p1`,
			params: []interface{}{
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			},
			err: nil,
		},
		{
			sql: "SELECT ?, ?",
			args: []interface{}{
				100,
				200,
			},
			yql: `-- origin query with positional args replacement
SELECT $p0, $p1`,
			params: []interface{}{
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			},
			err: nil,
		},
		{
			sql: "SELECT ?, ?",
			args: []interface{}{
				100,
				200,
			},
			yql: `-- origin query with positional args replacement
SELECT $p0, $p1`,
			params: []interface{}{
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			},
			err: nil,
		},
	} {
		t.Run("", func(t *testing.T) {
			yql, params, err := b.RewriteQuery(tt.sql, tt.args...)
			if tt.err != nil {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.yql, yql)
				require.Equal(t, tt.params, params)
			}
		})
	}
}
