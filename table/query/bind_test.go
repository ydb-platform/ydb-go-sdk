package query

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestBind_ToYQL(t *testing.T) {
	for _, tt := range []struct {
		b         Bind
		q         string
		args      []interface{}
		expQuery  string
		expParams *table.QueryParameters
		expErr    error
	}{
		{
			b: NewBind(
				TablePathPrefix("/local/"),
				Declare(),
			),
			q: `
DECLARE $param1 AS Text;
DECLARE $param2 AS Text;
SELECT $param1, $param2`,
			args: []interface{}{
				sql.Named("param1", 100),
				sql.Named("$param2", 200),
			},
			expQuery: `-- bind TablePathPrefix
PRAGMA TablePathPrefix("/local/");

-- bind declares
DECLARE $param1 AS Int32;
DECLARE $param2 AS Int32;


DECLARE $param1 AS Text;
DECLARE $param2 AS Text;
SELECT $param1, $param2`,
			expParams: table.NewQueryParameters(
				table.ValueParam("$param1", types.Int32Value(100)),
				table.ValueParam("$param2", types.Int32Value(200)),
			),
		},
		{
			b: NewBind(
				TablePathPrefix("/local/"),
				Declare(),
			),
			q: `
DECLARE $param2 AS Text;
SELECT $param1, $param2`,
			args: []interface{}{
				sql.Named("param1", 100),
				sql.Named("$param2", 200),
			},
			expQuery: `-- bind TablePathPrefix
PRAGMA TablePathPrefix("/local/");

-- bind declares
DECLARE $param1 AS Int32;
DECLARE $param2 AS Int32;


DECLARE $param2 AS Text;
SELECT $param1, $param2`,
			expParams: table.NewQueryParameters(
				table.ValueParam("$param1", types.Int32Value(100)),
				table.ValueParam("$param2", types.Int32Value(200)),
			),
		},
		{
			q:        "SELECT 1",
			expQuery: "SELECT 1",
		},
		{
			b: NewBind(Origin()),
			q: "SELECT 1",
			expQuery: `-- modified by ydb-go-sdk@v` + meta.Version + `
--   SELECT 1
SELECT 1`,
		},
		{
			b: NewBind(Origin()),
			q: `
SELECT 1`,
			expQuery: `-- modified by ydb-go-sdk@v` + meta.Version + `
--   SELECT 1
SELECT 1`,
		},
		{
			b: NewBind(Positional()),
			q: "SELECT ?, ?",
			args: []interface{}{
				1,
			},
			expErr: errInconsistentArgs,
		},
		{
			b: NewBind(Positional()),
			q: "SELECT ?, ?",
			args: []interface{}{
				100,
				200,
			},
			expQuery: `-- origin query with positional args replacement
SELECT $p0, $p1`,
			expParams: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			),
		},
		{
			b: NewBind(Positional()),
			q: "SELECT ?, ?",
			args: []interface{}{
				100,
			},
			expQuery: `-- origin query with positional args replacement
SELECT $p0, $p1`,
			expParams: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(100)),
			),
			expErr: errInconsistentArgs,
		},
		{
			b: NewBind(Positional()),
			q: "SELECT ?, ?, ?",
			args: []interface{}{
				100,
				200,
			},
			expQuery: `-- origin query with positional args replacement
SELECT $p0, $p1`,
			expParams: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			),
			expErr: errInconsistentArgs,
		},
		{
			b: NewBind(Positional()),
			q: `
SELECT ?, ?`,
			args: []interface{}{
				100,
				200,
			},
			expQuery: `-- origin query with positional args replacement
SELECT $p0, $p1`,
			expParams: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			),
		},
		{
			b: NewBind(
				TablePathPrefix("/local/"),
				Positional(),
			),
			q: "SELECT ?, ?",
			args: []interface{}{
				100,
				200,
			},
			expQuery: `-- bind TablePathPrefix
PRAGMA TablePathPrefix("/local/");

-- origin query with positional args replacement
SELECT $p0, $p1`,
			expParams: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			),
		},
		{
			b: NewBind(
				TablePathPrefix("/local/"),
				Declare(),
				Positional(),
			),
			q: "SELECT ?, ?",
			args: []interface{}{
				100,
				200,
			},
			expQuery: `-- bind TablePathPrefix
PRAGMA TablePathPrefix("/local/");

-- bind declares
DECLARE $p0 AS Int32;
DECLARE $p1 AS Int32;

-- origin query with positional args replacement
SELECT $p0, $p1`,
			expParams: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			),
		},
		{
			b: NewBind(Numeric()),
			q: "SELECT $1, $2",
			args: []interface{}{
				1,
			},
			expErr: errInconsistentArgs,
		},
		{
			b: NewBind(Numeric()),
			q: "SELECT $1, $2",
			args: []interface{}{
				100,
				200,
			},
			expQuery: `-- origin query with numeric args replacement
SELECT $p0, $p1`,
			expParams: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			),
		},
		{
			b: NewBind(
				Declare(),
				Numeric(),
			),
			q: "SELECT $1, $2",
			args: []interface{}{
				100,
				200,
			},
			expQuery: `-- bind declares
DECLARE $p0 AS Int32;
DECLARE $p1 AS Int32;

-- origin query with numeric args replacement
SELECT $p0, $p1`,
			expParams: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			),
		},
		{
			b: NewBind(Numeric()),
			q: `
SELECT $1, $2`,
			args: []interface{}{
				100,
				200,
			},
			expQuery: `-- origin query with numeric args replacement
SELECT $p0, $p1`,
			expParams: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			),
		},
		{
			b: NewBind(
				TablePathPrefix("/local/"),
				Numeric(),
			),
			q: "SELECT $1, $2",
			args: []interface{}{
				100,
				200,
			},
			expQuery: `-- bind TablePathPrefix
PRAGMA TablePathPrefix("/local/");

-- origin query with numeric args replacement
SELECT $p0, $p1`,
			expParams: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			),
		},
		{
			b: NewBind(
				TablePathPrefix("/local/"),
				Declare(),
				Numeric(),
			),
			q: "SELECT $1, $2",
			args: []interface{}{
				100,
				200,
			},
			expQuery: `-- bind TablePathPrefix
PRAGMA TablePathPrefix("/local/");

-- bind declares
DECLARE $p0 AS Int32;
DECLARE $p1 AS Int32;

-- origin query with numeric args replacement
SELECT $p0, $p1`,
			expParams: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			),
		},
		{
			b: NewBind(TablePathPrefix("/local/")),
			q: "SELECT 1",
			expQuery: `-- bind TablePathPrefix
PRAGMA TablePathPrefix("/local/");

SELECT 1`,
		},
		{
			b: NewBind(
				TablePathPrefix("/local/"),
				Declare(),
			),
			q: "SELECT 1",
			expQuery: `-- bind TablePathPrefix
PRAGMA TablePathPrefix("/local/");

SELECT 1`,
		},
		{
			b: NewBind(
				TablePathPrefix("/local/"),
				Declare(),
			),
			q: "SELECT $param1, $param2",
			args: []interface{}{
				sql.Named("param1", 100),
				sql.Named("$param2", 200),
			},
			expQuery: `-- bind TablePathPrefix
PRAGMA TablePathPrefix("/local/");

-- bind declares
DECLARE $param1 AS Int32;
DECLARE $param2 AS Int32;

SELECT $param1, $param2`,
			expParams: table.NewQueryParameters(
				table.ValueParam("$param1", types.Int32Value(100)),
				table.ValueParam("$param2", types.Int32Value(200)),
			),
		},
	} {
		t.Run("", func(t *testing.T) {
			query, params, err := tt.b.ToYQL(tt.q, tt.args...)
			if tt.expErr != nil {
				require.ErrorIs(t, err, tt.expErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, removeWindowsCarriageReturn(tt.expQuery), removeWindowsCarriageReturn(query))
				require.Equal(t, tt.expParams.String(), params.String())
			}
		})
	}
}

func removeWindowsCarriageReturn(s string) string {
	return strings.ReplaceAll(s, "\r", "")
}
