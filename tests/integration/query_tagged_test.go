//go:build integration

package integration

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/types"
)

func TestQueryTaggedResults(t *testing.T) {
	scope := newScope(t)
	db := scope.Driver()
	for _, tt := range []struct {
		name       string
		expression string
		typeYql    string
		valueYql   string
	}{
		{"scalar", `AsTagged(42, "tag")`, `Tagged<Int32,"tag">`, `AsTagged(42,"tag")`},
		{"optional", `Just(AsTagged(42, "tag"))`, `Optional<Tagged<Int32,"tag">>`, `Just(AsTagged(42,"tag"))`},
		{
			"tagged_optional", `AsTagged(Just(42), "tag")`, `Tagged<Optional<Int32>,"tag">`,
			`AsTagged(Just(42),"tag")`,
		},
		{
			"null", `AsTagged(Nothing(Int32?), "tag")`, `Tagged<Optional<Int32>,"tag">`,
			`AsTagged(Nothing(Optional<Int32>),"tag")`,
		},
		{
			"optional_null", `Nothing(OptionalType(TypeOf(AsTagged(42, "tag"))))`,
			`Optional<Tagged<Int32,"tag">>`, `Nothing(Optional<Tagged<Int32,"tag">>)`,
		},
		{
			"nested_null", `Just(AsTagged(Nothing(Int32?), "tag"))`, `Optional<Tagged<Optional<Int32>,"tag">>`,
			`Just(AsTagged(Nothing(Optional<Int32>),"tag"))`,
		},
		{"list", `[AsTagged(42, "tag")]`, `List<Tagged<Int32,"tag">>`, `[AsTagged(42,"tag")]`},
		{
			"nested", `AsTagged(AsTagged(42, "inner"), "outer")`, `Tagged<Tagged<Int32,"inner">,"outer">`,
			`AsTagged(AsTagged(42,"inner"),"outer")`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			yql := "SELECT " + tt.expression + " AS value;"
			t.Run("Query", func(t *testing.T) {
				res, err := db.Query().Query(scope.Ctx, yql)
				require.NoError(t, err)
				defer func() { require.NoError(t, res.Close(scope.Ctx)) }()
				rs, err := res.NextResultSet(scope.Ctx)
				require.NoError(t, err)
				require.Equal(t, tt.typeYql, rs.ColumnTypes()[0].Yql())
				row, err := rs.NextRow(scope.Ctx)
				require.NoError(t, err)
				values := row.Values()
				require.Len(t, values, 1)
				require.Equal(t, tt.typeYql, values[0].Type().Yql())
				require.Equal(t, tt.valueYql, values[0].Yql())
				var tagged types.Value
				require.NoError(t, row.Scan(&tagged))
				require.Equal(t, values[0], tagged)
				var named types.Value
				require.NoError(t, row.ScanNamed(query.Named("value", &named)))
				require.Equal(t, values[0], named)
				var dst struct {
					Value types.Value `sql:"value"`
				}
				require.NoError(t, row.ScanStruct(&dst))
				require.Equal(t, values[0], dst.Value)
				switch tt.name {
				case "scalar", "nested":
					var n int32
					require.NoError(t, row.Scan(&n))
					require.EqualValues(t, 42, n)
					n = 0
					require.NoError(t, row.ScanNamed(query.Named("value", &n)))
					require.EqualValues(t, 42, n)
					var scalar struct {
						Value int32 `sql:"value"`
					}
					require.NoError(t, row.ScanStruct(&scalar))
					require.EqualValues(t, 42, scalar.Value)
					var invalid bool
					require.Error(t, row.Scan(&invalid))
				case "optional", "tagged_optional", "null", "optional_null":
					var n *int32
					require.NoError(t, row.Scan(&n))
					if tt.name == "null" || tt.name == "optional_null" {
						require.Nil(t, n)
					} else {
						require.NotNil(t, n)
						require.EqualValues(t, 42, *n)
					}
					var named *int32
					require.NoError(t, row.ScanNamed(query.Named("value", &named)))
					require.Equal(t, n, named)
					var dst struct {
						Value *int32 `sql:"value"`
					}
					require.NoError(t, row.ScanStruct(&dst))
					require.Equal(t, n, dst.Value)
				case "nested_null":
					var n **int32
					require.NoError(t, row.Scan(&n))
					require.NotNil(t, n)
					require.Nil(t, *n)
					var named **int32
					require.NoError(t, row.ScanNamed(query.Named("value", &named)))
					require.Equal(t, n, named)
					var dst struct {
						Value **int32 `sql:"value"`
					}
					require.NoError(t, row.ScanStruct(&dst))
					require.Equal(t, n, dst.Value)
				case "list":
					var numbers []int32
					require.NoError(t, row.Scan(&numbers))
					require.Equal(t, []int32{42}, numbers)
					var named []int32
					require.NoError(t, row.ScanNamed(query.Named("value", &named)))
					require.Equal(t, numbers, named)
					var dst struct {
						Value []int32 `sql:"value"`
					}
					require.NoError(t, row.ScanStruct(&dst))
					require.Equal(t, numbers, dst.Value)
				}
				_, err = rs.NextRow(scope.Ctx)
				require.ErrorIs(t, err, io.EOF)
			})
			t.Run("QueryRow", func(t *testing.T) {
				row, err := db.Query().QueryRow(scope.Ctx, yql)
				require.NoError(t, err)
				values := row.Values()
				require.Len(t, values, 1)
				require.Equal(t, tt.typeYql, values[0].Type().Yql())
				require.Equal(t, tt.valueYql, values[0].Yql())
				var tagged types.Value
				require.NoError(t, row.Scan(&tagged))
				require.Equal(t, values[0], tagged)
				var named types.Value
				require.NoError(t, row.ScanNamed(query.Named("value", &named)))
				require.Equal(t, values[0], named)
				var dst struct {
					Value types.Value `sql:"value"`
				}
				require.NoError(t, row.ScanStruct(&dst))
				require.Equal(t, values[0], dst.Value)
				switch tt.name {
				case "scalar", "nested":
					var n int32
					require.NoError(t, row.Scan(&n))
					require.EqualValues(t, 42, n)
					n = 0
					require.NoError(t, row.ScanNamed(query.Named("value", &n)))
					require.EqualValues(t, 42, n)
					var scalar struct {
						Value int32 `sql:"value"`
					}
					require.NoError(t, row.ScanStruct(&scalar))
					require.EqualValues(t, 42, scalar.Value)
					var invalid bool
					require.Error(t, row.Scan(&invalid))
				case "optional", "tagged_optional", "null", "optional_null":
					var n *int32
					require.NoError(t, row.Scan(&n))
					if tt.name == "null" || tt.name == "optional_null" {
						require.Nil(t, n)
					} else {
						require.NotNil(t, n)
						require.EqualValues(t, 42, *n)
					}
					var named *int32
					require.NoError(t, row.ScanNamed(query.Named("value", &named)))
					require.Equal(t, n, named)
					var dst struct {
						Value *int32 `sql:"value"`
					}
					require.NoError(t, row.ScanStruct(&dst))
					require.Equal(t, n, dst.Value)
				case "nested_null":
					var n **int32
					require.NoError(t, row.Scan(&n))
					require.NotNil(t, n)
					require.Nil(t, *n)
					var named **int32
					require.NoError(t, row.ScanNamed(query.Named("value", &named)))
					require.Equal(t, n, named)
					var dst struct {
						Value **int32 `sql:"value"`
					}
					require.NoError(t, row.ScanStruct(&dst))
					require.Equal(t, n, dst.Value)
				case "list":
					var numbers []int32
					require.NoError(t, row.Scan(&numbers))
					require.Equal(t, []int32{42}, numbers)
					var named []int32
					require.NoError(t, row.ScanNamed(query.Named("value", &named)))
					require.Equal(t, numbers, named)
					var dst struct {
						Value []int32 `sql:"value"`
					}
					require.NoError(t, row.ScanStruct(&dst))
					require.Equal(t, numbers, dst.Value)
				}
			})
			t.Run("QueryResultSet", func(t *testing.T) {
				rs, err := db.Query().QueryResultSet(scope.Ctx, yql)
				require.NoError(t, err)
				defer func() { require.NoError(t, rs.Close(scope.Ctx)) }()
				require.Equal(t, tt.typeYql, rs.ColumnTypes()[0].Yql())
				row, err := rs.NextRow(scope.Ctx)
				require.NoError(t, err)
				values := row.Values()
				require.Len(t, values, 1)
				require.Equal(t, tt.typeYql, values[0].Type().Yql())
				require.Equal(t, tt.valueYql, values[0].Yql())
				var tagged types.Value
				require.NoError(t, row.Scan(&tagged))
				require.Equal(t, values[0], tagged)
				var named types.Value
				require.NoError(t, row.ScanNamed(query.Named("value", &named)))
				require.Equal(t, values[0], named)
				var dst struct {
					Value types.Value `sql:"value"`
				}
				require.NoError(t, row.ScanStruct(&dst))
				require.Equal(t, values[0], dst.Value)
				switch tt.name {
				case "scalar", "nested":
					var n int32
					require.NoError(t, row.Scan(&n))
					require.EqualValues(t, 42, n)
					n = 0
					require.NoError(t, row.ScanNamed(query.Named("value", &n)))
					require.EqualValues(t, 42, n)
					var scalar struct {
						Value int32 `sql:"value"`
					}
					require.NoError(t, row.ScanStruct(&scalar))
					require.EqualValues(t, 42, scalar.Value)
					var invalid bool
					require.Error(t, row.Scan(&invalid))
				case "optional", "tagged_optional", "null", "optional_null":
					var n *int32
					require.NoError(t, row.Scan(&n))
					if tt.name == "null" || tt.name == "optional_null" {
						require.Nil(t, n)
					} else {
						require.NotNil(t, n)
						require.EqualValues(t, 42, *n)
					}
					var named *int32
					require.NoError(t, row.ScanNamed(query.Named("value", &named)))
					require.Equal(t, n, named)
					var dst struct {
						Value *int32 `sql:"value"`
					}
					require.NoError(t, row.ScanStruct(&dst))
					require.Equal(t, n, dst.Value)
				case "nested_null":
					var n **int32
					require.NoError(t, row.Scan(&n))
					require.NotNil(t, n)
					require.Nil(t, *n)
					var named **int32
					require.NoError(t, row.ScanNamed(query.Named("value", &named)))
					require.Equal(t, n, named)
					var dst struct {
						Value **int32 `sql:"value"`
					}
					require.NoError(t, row.ScanStruct(&dst))
					require.Equal(t, n, dst.Value)
				case "list":
					var numbers []int32
					require.NoError(t, row.Scan(&numbers))
					require.Equal(t, []int32{42}, numbers)
					var named []int32
					require.NoError(t, row.ScanNamed(query.Named("value", &named)))
					require.Equal(t, numbers, named)
					var dst struct {
						Value []int32 `sql:"value"`
					}
					require.NoError(t, row.ScanStruct(&dst))
					require.Equal(t, numbers, dst.Value)
				}
				_, err = rs.NextRow(scope.Ctx)
				require.ErrorIs(t, err, io.EOF)
			})
		})
	}
}
