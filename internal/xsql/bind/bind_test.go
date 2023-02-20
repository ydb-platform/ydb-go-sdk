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

func TestBindings_Bind(t *testing.T) {
	for _, tt := range []struct {
		b         Bindings
		q         string
		args      []driver.NamedValue
		expQuery  string
		expParams *table.QueryParameters
		expErr    error
	}{
		{
			b:        Bindings{},
			q:        "SELECT 1",
			args:     nil,
			expQuery: "SELECT 1",
		},
		{
			b: Bindings{},
			q: "SELECT $1, ?",
			args: []driver.NamedValue{
				{Value: 1},
			},
			expErr: errUnknownQueryType,
		},
		{
			b: Bindings{},
			q: "SELECT $1, ?, $p1",
			args: []driver.NamedValue{
				{Value: 1},
			},
			expErr: errUnknownQueryType,
		},
		{
			b: Bindings{},
			q: "SELECT ?, $p1",
			args: []driver.NamedValue{
				{Value: 1},
			},
			expErr: errUnknownQueryType,
		},
		{
			b: Bindings{},
			q: "SELECT $1, $p1",
			args: []driver.NamedValue{
				{Value: 1},
			},
			expErr: errUnknownQueryType,
		},
		{
			b: Bindings{
				TablePathPrefix: "/local/",
			},
			q:    "SELECT 1",
			args: nil,
			expQuery: `-- modified by ydb-go-sdk@v` + meta.Version + `
--
-- source query:
--   SELECT 1
--
PRAGMA TablePathPrefix("/local/");
SELECT 1`,
			expParams: nil,
			expErr:    nil,
		},
		{
			b: Bindings{
				TablePathPrefix: "/local/",
				AllowBindParams: true,
			},
			q: "SELECT $1",
			args: []driver.NamedValue{
				{Value: 1},
			},
			expQuery: `-- modified by ydb-go-sdk@v` + meta.Version + `
--
-- source query:
--   SELECT $1
--
PRAGMA TablePathPrefix("/local/");
DECLARE $p0 AS Int32;
SELECT $p0`,
			expParams: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(1)),
			),
			expErr: nil,
		},
		{
			b: Bindings{
				TablePathPrefix: "/local/",
				AllowBindParams: true,
			},
			q: "SELECT $1, $2, $3",
			args: []driver.NamedValue{
				{Value: 1},
				{Value: uint64(2)},
				{Value: true},
			},
			expQuery: `-- modified by ydb-go-sdk@v` + meta.Version + `
--
-- source query:
--   SELECT $1, $2, $3
--
PRAGMA TablePathPrefix("/local/");
DECLARE $p0 AS Int32;
DECLARE $p1 AS Uint64;
DECLARE $p2 AS Bool;
SELECT $p0, $p1, $p2`,
			expParams: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(1)),
				table.ValueParam("$p1", types.Uint64Value(2)),
				table.ValueParam("$p2", types.BoolValue(true)),
			),
			expErr: nil,
		},
		{
			b: Bindings{
				TablePathPrefix: "/local/",
				AllowBindParams: true,
			},
			q: "SELECT $2, $1, $3, $1, $2",
			args: []driver.NamedValue{
				{Value: 1},
				{Value: uint64(2)},
				{Value: true},
			},
			expQuery: `-- modified by ydb-go-sdk@v` + meta.Version + `
--
-- source query:
--   SELECT $2, $1, $3, $1, $2
--
PRAGMA TablePathPrefix("/local/");
DECLARE $p0 AS Int32;
DECLARE $p1 AS Uint64;
DECLARE $p2 AS Bool;
SELECT $p1, $p0, $p2, $p0, $p1`,
			expParams: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(1)),
				table.ValueParam("$p1", types.Uint64Value(2)),
				table.ValueParam("$p2", types.BoolValue(true)),
			),
			expErr: nil,
		},
		{
			b: Bindings{
				TablePathPrefix: "/local/",
				AllowBindParams: true,
			},
			q: "SELECT ?",
			args: []driver.NamedValue{
				{
					Value: 1,
				},
			},
			expQuery: `-- modified by ydb-go-sdk@v` + meta.Version + `
--
-- source query:
--   SELECT ?
--
PRAGMA TablePathPrefix("/local/");
DECLARE $p0 AS Int32;
SELECT $p0`,
			expParams: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(1)),
			),
			expErr: nil,
		},
		{
			b: Bindings{
				TablePathPrefix: "/local/",
				AllowBindParams: true,
			},
			q: "SELECT ?, ?, ?",
			args: []driver.NamedValue{
				{Value: 1},
				{Value: 2},
				{Value: 3},
			},
			expQuery: `-- modified by ydb-go-sdk@v` + meta.Version + `
--
-- source query:
--   SELECT ?, ?, ?
--
PRAGMA TablePathPrefix("/local/");
DECLARE $p0 AS Int32;
DECLARE $p1 AS Int32;
DECLARE $p2 AS Int32;
SELECT $p0, $p1, $p2`,
			expParams: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(1)),
				table.ValueParam("$p1", types.Int32Value(2)),
				table.ValueParam("$p2", types.Int32Value(3)),
			),
			expErr: nil,
		},
		{
			b: Bindings{
				TablePathPrefix: "/local/",
				AllowBindParams: true,
			},
			q: "SELECT ?, ?, ?",
			args: []driver.NamedValue{
				{Value: 1},
				{Value: int64(2)},
				{Value: true},
			},
			expQuery: `-- modified by ydb-go-sdk@v` + meta.Version + `
--
-- source query:
--   SELECT ?, ?, ?
--
PRAGMA TablePathPrefix("/local/");
DECLARE $p0 AS Int32;
DECLARE $p1 AS Int64;
DECLARE $p2 AS Bool;
SELECT $p0, $p1, $p2`,
			expParams: table.NewQueryParameters(
				table.ValueParam("$p0", types.Int32Value(1)),
				table.ValueParam("$p1", types.Int64Value(2)),
				table.ValueParam("$p2", types.BoolValue(true)),
			),
			expErr: nil,
		},
	} {
		t.Run("", func(t *testing.T) {
			query, params, err := tt.b.Bind(tt.q, tt.args...)
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
