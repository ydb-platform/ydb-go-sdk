package bind

import (
	"database/sql/driver"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func named(name string, value interface{}) driver.NamedValue {
	return driver.NamedValue{
		Name:  name,
		Value: value,
	}
}

func TestBindings_Bind(t *testing.T) {
	for _, tt := range []struct {
		b         Bind
		q         string
		args      []driver.NamedValue
		expQuery  string
		expParams *table.QueryParameters
		expErr    error
	}{
		{
			q:        "SELECT 1",
			expQuery: "SELECT 1",
		},
		{
			b: Positional(),
			q: "SELECT ?, ?",
			args: []driver.NamedValue{
				{Value: 1},
			},
			expErr: errArgsCount,
		},
		{
			b: Positional(),
			q: "SELECT ?, ?",
			args: []driver.NamedValue{
				{Value: 100},
				{Value: 200},
			},
			expQuery: `-- modified by ydb-go-sdk@v` + meta.Version + ` (bind type = Positional)
--   SELECT ?, ?

-- bind declares
DECLARE $p0 AS Int32;
DECLARE $p1 AS Int32;

-- origin query with normalized args
SELECT $p0, $p1`,
			expParams: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			),
		},
		{
			b: Positional(),
			q: `
-- some comment with positional args like ?
SELECT ?, ?`,
			args: []driver.NamedValue{
				{Value: 100},
				{Value: 200},
			},
			expQuery: `-- modified by ydb-go-sdk@v` + meta.Version + ` (bind type = Positional)
--   
--   -- some comment with positional args like ?
--   SELECT ?, ?

-- bind declares
DECLARE $p0 AS Int32;
DECLARE $p1 AS Int32;

-- origin query with normalized args
SELECT $p0, $p1`,
			expParams: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			),
		},
		{
			b: Positional().WithTablePathPrefix("/local/"),
			q: "SELECT ?, ?",
			args: []driver.NamedValue{
				{Value: 100},
				{Value: 200},
			},
			expQuery: `-- modified by ydb-go-sdk@v` + meta.Version + ` (bind type = TablePathPrefix|Positional)
--   SELECT ?, ?

PRAGMA TablePathPrefix("/local/");

-- bind declares
DECLARE $p0 AS Int32;
DECLARE $p1 AS Int32;

-- origin query with normalized args
SELECT $p0, $p1`,
			expParams: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			),
		},
		{
			b: Numeric(),
			q: "SELECT $1, $2",
			args: []driver.NamedValue{
				{Value: 1},
			},
			expErr: errArgsCount,
		},
		{
			b: Numeric(),
			q: "SELECT $1, $2",
			args: []driver.NamedValue{
				{Value: 100},
				{Value: 200},
			},
			expQuery: `-- modified by ydb-go-sdk@v` + meta.Version + ` (bind type = Numeric)
--   SELECT $1, $2

-- bind declares
DECLARE $p0 AS Int32;
DECLARE $p1 AS Int32;

-- origin query with normalized args
SELECT $p0, $p1`,
			expParams: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			),
		},
		{
			b: Numeric(),
			q: `
-- some comment with numeric args like $3
SELECT $1, $2`,
			args: []driver.NamedValue{
				{Value: 100},
				{Value: 200},
			},
			expQuery: `-- modified by ydb-go-sdk@v` + meta.Version + ` (bind type = Numeric)
--   
--   -- some comment with numeric args like $3
--   SELECT $1, $2

-- bind declares
DECLARE $p0 AS Int32;
DECLARE $p1 AS Int32;

-- origin query with normalized args
SELECT $p0, $p1`,
			expParams: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			),
		},
		{
			b: Numeric().WithTablePathPrefix("/local/"),
			q: "SELECT $1, $2",
			args: []driver.NamedValue{
				{Value: 100},
				{Value: 200},
			},
			expQuery: `-- modified by ydb-go-sdk@v` + meta.Version + ` (bind type = TablePathPrefix|Numeric)
--   SELECT $1, $2

PRAGMA TablePathPrefix("/local/");

-- bind declares
DECLARE $p0 AS Int32;
DECLARE $p1 AS Int32;

-- origin query with normalized args
SELECT $p0, $p1`,
			expParams: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(100)),
				table.ValueParam("$p1", types.Int32Value(200)),
			),
		},
		{
			b: NoBind().WithTablePathPrefix("/local/"),
			q: "SELECT 1",
			expQuery: `-- modified by ydb-go-sdk@v` + meta.Version + ` (bind type = TablePathPrefix)
--   SELECT 1

PRAGMA TablePathPrefix("/local/");

-- origin query
SELECT 1`,
		},
		{
			b: Declare().WithTablePathPrefix("/local/"),
			q: "SELECT $param1, $param2",
			args: []driver.NamedValue{
				named("param1", 100),
				named("$param2", 200),
			},
			expQuery: `-- modified by ydb-go-sdk@v` + meta.Version + ` (bind type = TablePathPrefix|Declare)
--   SELECT $param1, $param2

PRAGMA TablePathPrefix("/local/");

-- bind declares
DECLARE $param1 AS Int32;
DECLARE $param2 AS Int32;

-- origin query
SELECT $param1, $param2`,
			expParams: table.NewQueryParameters(
				table.ValueParam("$param1", types.Int32Value(100)),
				table.ValueParam("$param2", types.Int32Value(200)),
			),
		},
		{
			b: Declare().WithTablePathPrefix("/local/"),
			q: `
DECLARE $param1 AS Text;
DECLARE $param2 AS Text;
SELECT $param1, $param2`,
			args: []driver.NamedValue{
				named("param1", 100),
				named("$param2", 200),
			},
			expQuery: `-- modified by ydb-go-sdk@v` + meta.Version + ` (bind type = TablePathPrefix|Declare)
--   
--   DECLARE $param1 AS Text;
--   DECLARE $param2 AS Text;
--   SELECT $param1, $param2

PRAGMA TablePathPrefix("/local/");

-- bind declares
DECLARE $param1 AS Int32;
DECLARE $param2 AS Int32;

-- origin query
DECLARE $param1 AS Text;
DECLARE $param2 AS Text;
SELECT $param1, $param2`,
			expParams: table.NewQueryParameters(
				table.ValueParam("$param1", types.Int32Value(100)),
				table.ValueParam("$param2", types.Int32Value(200)),
			),
		},
		{
			b: Declare().WithTablePathPrefix("/local/"),
			q: `
DECLARE $param2 AS Text;
SELECT $param1, $param2`,
			args: []driver.NamedValue{
				named("param1", 100),
				named("$param2", 200),
			},
			expQuery: `-- modified by ydb-go-sdk@v` + meta.Version + ` (bind type = TablePathPrefix|Declare)
--   
--   DECLARE $param2 AS Text;
--   SELECT $param1, $param2

PRAGMA TablePathPrefix("/local/");

-- bind declares
DECLARE $param1 AS Int32;
DECLARE $param2 AS Int32;

-- origin query
DECLARE $param2 AS Text;
SELECT $param1, $param2`,
			expParams: table.NewQueryParameters(
				table.ValueParam("$param1", types.Int32Value(100)),
				table.ValueParam("$param2", types.Int32Value(200)),
			),
		},
	} {
		t.Run("", func(t *testing.T) {
			query, params, err := tt.b.bind(tt.q, tt.args...)
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

func Test_removeComments(t *testing.T) {
	for _, tt := range []struct {
		src string
		dst string
	}{
		{
			src: `
-- some comment
SELECT 1;`,
			dst: `

SELECT 1;`,
		},
		{
			src: `SELECT 1; -- some comment`,
			dst: `SELECT 1; `,
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.dst, removeComments(tt.src))
		})
	}
}

func Test_removeEmptyLines(t *testing.T) {
	for _, tt := range []struct {
		src string
		dst string
	}{
		{
			src: `

  
test
 

`,
			dst: `test`,
		},
		{
			src: `

  
   test
 

`,
			dst: `   test`,
		},
		{
			src: `

  
   test
 

end`,
			dst: `   test
end`,
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.dst, removeEmptyLines(tt.src))
		})
	}
}
