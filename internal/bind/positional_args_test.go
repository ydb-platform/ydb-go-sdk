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
		args   []any
		yql    string
		params []any
		err    error
	}{
		{
			sql: `SELECT ?, ?`,
			args: []any{
				100,
				200,
			},
			yql: `-- origin query with positional args replacement
SELECT $p0, $p1`,
			params: []any{
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			},
		},
		{
			sql: `SELECT ?, ?`,
			args: []any{
				100,
			},
			err: ErrInconsistentArgs,
		},
		{
			sql: `SELECT ?, "?"`,
			args: []any{
				100,
			},
			yql: `-- origin query with positional args replacement
SELECT $p0, "?"`,
			params: []any{
				table.ValueParam("$p0", types.Int32Value(100)),
			},
		},
		{
			sql: `SELECT ?, '?'`,
			args: []any{
				100,
			},
			yql: `-- origin query with positional args replacement
SELECT $p0, '?'`,
			params: []any{
				table.ValueParam("$p0", types.Int32Value(100)),
			},
		},
		{
			sql: "SELECT ?, `?`",
			args: []any{
				100,
			},
			yql: "-- origin query with positional args replacement\nSELECT $p0, `?`",
			params: []any{
				table.ValueParam("$p0", types.Int32Value(100)),
			},
		},
		{
			sql: "SELECT ?, $1, $p0",
			err: ErrInconsistentArgs,
		},
		{
			sql: "SELECT ?, ?, ?",
			args: []any{
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
			params: []any{
				table.ValueParam("$p0", types.Int32Value(1)),
				table.ValueParam("$p1", types.TextValue("test")),
				table.ValueParam("$p2", types.ListValue(
					types.TextValue("test1"),
					types.TextValue("test2"),
					types.TextValue("test3"),
				)),
			},
		},
		{
			sql: "SELECT ?, ?, ?",
			args: []any{
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
			params: []any{
				table.ValueParam("$p0", types.Int32Value(1)),
				table.ValueParam("$p1", types.TextValue("test")),
				table.ValueParam("$p2", types.ListValue(
					types.TextValue("test1"),
					types.TextValue("test2"),
					types.TextValue("test3"),
				)),
			},
		},
		{
			sql: "SELECT a, b, c WHERE id = ? AND date < ? AND value IN (?)",
			args: []any{
				1, now, []string{"3"},
			},
			yql: `-- origin query with positional args replacement
SELECT a, b, c WHERE id = $p0 AND date < $p1 AND value IN ($p2)`,
			params: []any{
				table.ValueParam("$p0", types.Int32Value(1)),
				table.ValueParam("$p1", types.TimestampValueFromTime(now)),
				table.ValueParam("$p2", types.ListValue(types.TextValue("3"))),
			},
		},
		{
			sql: "SELECT 1",
			yql: "SELECT 1",
		},
		{
			sql: `
SELECT 1`,
			yql: `
SELECT 1`,
		},
		{
			sql: "SELECT ?, ?",
			args: []any{
				1,
			},
			err: ErrInconsistentArgs,
		},
		{
			sql: "SELECT ?, ? -- some comment with ?",
			args: []any{
				100,
				200,
			},
			yql: `-- origin query with positional args replacement
SELECT $p0, $p1 -- some comment with ?`,
			params: []any{
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			},
		},
		{
			sql: "SELECT ? /* some comment with ? */, ?",
			args: []any{
				100,
				200,
			},
			yql: `-- origin query with positional args replacement
SELECT $p0 /* some comment with ? */, $p1`,
			params: []any{
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			},
		},
		{
			sql: "SELECT ?, ? -- some comment with ?",
			args: []any{
				100,
			},
			err: ErrInconsistentArgs,
		},
		{
			sql: "SELECT ?, ?, ?",
			args: []any{
				100,
				200,
			},
			err: ErrInconsistentArgs,
		},
		{
			sql: `
SELECT ? /* some comment with ? */, ?`,
			args: []any{
				100,
				200,
			},
			yql: `-- origin query with positional args replacement

SELECT $p0 /* some comment with ? */, $p1`,
			params: []any{
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			},
		},
		{
			sql: "SELECT ?, ?",
			args: []any{
				100,
				200,
			},
			yql: `-- origin query with positional args replacement
SELECT $p0, $p1`,
			params: []any{
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			},
		},
		{
			sql: "SELECT ?, ?",
			args: []any{
				100,
				200,
			},
			yql: `-- origin query with positional args replacement
SELECT $p0, $p1`,
			params: []any{
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			yql, params, err := b.ToYdb(tt.sql, tt.args...)
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
