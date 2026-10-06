//go:build integration

package integration

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/types"
)

func TestQueryEmptyCollections(t *testing.T) {
	scope := newScope(t)
	client := scope.Driver().Query()

	for _, tt := range []struct {
		name     string
		literal  string
		typeYql  string
		valueYql string
	}{
		{"EmptyList", "[]", "EmptyList", "[]"},
		{"EmptyDict", "AsDict()", "EmptyDict", "{}"},
		{"TypedList", "CAST([] AS List<Int32>)", "List<Int32>", "[]"},
		{"TypedDict", "CAST(AsDict() AS Dict<Utf8,Int32>)", "Dict<Utf8,Int32>", "{}"},
		{"TypedSet", "CAST(AsDict() AS Dict<Utf8,Void>)", "Set<Utf8>", "{}"},
		{"NestedList", "[[]]", "List<EmptyList>", "[[]]"},
		{"NestedDict", "[AsDict()]", "List<EmptyDict>", "[{}]"},
		{"Tuple", "AsTuple([], AsDict())", "Tuple<EmptyList,EmptyDict>", "([],{})"},
		{"OptionalList", "Just([])", "Optional<EmptyList>", "Just([])"},
		{"OptionalDict", "Just(AsDict())", "Optional<EmptyDict>", "Just({})"},
		{"NullList", "Nothing(OptionalType(TypeOf([])))", "Optional<EmptyList>", "Nothing(Optional<EmptyList>)"},
		{"NullDict", "Nothing(OptionalType(TypeOf(AsDict())))", "Optional<EmptyDict>", "Nothing(Optional<EmptyDict>)"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			q := "SELECT " + tt.literal + " AS value;"
			t.Run("Query", func(t *testing.T) {
				res, err := client.Query(scope.Ctx, q)
				require.NoError(t, err)
				defer func() { require.NoError(t, res.Close(scope.Ctx)) }()
				rs, err := res.NextResultSet(scope.Ctx)
				require.NoError(t, err)
				require.Equal(t, []string{"value"}, rs.Columns())
				require.Len(t, rs.ColumnTypes(), 1)
				require.Equal(t, tt.typeYql, rs.ColumnTypes()[0].Yql())
				row, err := rs.NextRow(scope.Ctx)
				require.NoError(t, err)
				values := row.Values()
				require.Len(t, values, 1)
				require.Equal(t, tt.typeYql, values[0].Type().Yql())
				require.Equal(t, tt.valueYql, values[0].Yql())

				var scanned types.Value
				require.NoError(t, row.Scan(&scanned))
				require.Equal(t, values[0], scanned)
				var named types.Value
				require.NoError(t, row.ScanNamed(query.Named("value", &named)))
				require.Equal(t, values[0], named)
				var dst struct {
					Value types.Value `sql:"value"`
				}
				require.NoError(t, row.ScanStruct(&dst))
				require.Equal(t, values[0], dst.Value)

				switch tt.name {
				case "EmptyList", "TypedList", "OptionalList":
					items, err := types.ListItems(types.Unwrap(scanned))
					require.NoError(t, err)
					require.Empty(t, items)
				case "EmptyDict", "TypedDict", "OptionalDict":
					items, err := types.DictValues(types.Unwrap(scanned))
					require.NoError(t, err)
					require.Empty(t, items)
				case "TypedSet":
					var items []string
					require.NoError(t, row.Scan(&items))
					require.Empty(t, items)
					var named []string
					require.NoError(t, row.ScanNamed(query.Named("value", &named)))
					require.Empty(t, named)
					var dst struct {
						Value []string `sql:"value"`
					}
					require.NoError(t, row.ScanStruct(&dst))
					require.Empty(t, dst.Value)
				case "NestedList":
					items, err := types.ListItems(scanned)
					require.NoError(t, err)
					require.Len(t, items, 1)
					inner, err := types.ListItems(items[0])
					require.NoError(t, err)
					require.Empty(t, inner)
				case "NestedDict":
					items, err := types.ListItems(scanned)
					require.NoError(t, err)
					require.Len(t, items, 1)
					inner, err := types.DictValues(items[0])
					require.NoError(t, err)
					require.Empty(t, inner)
				case "Tuple":
					items, err := types.TupleItems(scanned)
					require.NoError(t, err)
					require.Len(t, items, 2)
					list, err := types.ListItems(items[0])
					require.NoError(t, err)
					require.Empty(t, list)
					dict, err := types.DictValues(items[1])
					require.NoError(t, err)
					require.Empty(t, dict)
				case "NullList", "NullDict":
					require.True(t, types.IsNull(scanned))
				}
				if !types.IsNull(scanned) {
					var incompatible int
					require.Error(t, row.Scan(&incompatible))
				}
				_, err = rs.NextRow(scope.Ctx)
				require.ErrorIs(t, err, io.EOF)
				_, err = res.NextResultSet(scope.Ctx)
				require.ErrorIs(t, err, io.EOF)
			})
			t.Run("QueryRow", func(t *testing.T) {
				row, err := client.QueryRow(scope.Ctx, q)
				require.NoError(t, err)
				values := row.Values()
				require.Len(t, values, 1)
				require.Equal(t, tt.typeYql, values[0].Type().Yql())
				require.Equal(t, tt.valueYql, values[0].Yql())

				var scanned types.Value
				require.NoError(t, row.Scan(&scanned))
				require.Equal(t, values[0], scanned)
				var named types.Value
				require.NoError(t, row.ScanNamed(query.Named("value", &named)))
				require.Equal(t, values[0], named)
				var dst struct {
					Value types.Value `sql:"value"`
				}
				require.NoError(t, row.ScanStruct(&dst))
				require.Equal(t, values[0], dst.Value)

				switch tt.name {
				case "EmptyList", "TypedList", "OptionalList":
					items, err := types.ListItems(types.Unwrap(scanned))
					require.NoError(t, err)
					require.Empty(t, items)
				case "EmptyDict", "TypedDict", "OptionalDict":
					items, err := types.DictValues(types.Unwrap(scanned))
					require.NoError(t, err)
					require.Empty(t, items)
				case "TypedSet":
					var items []string
					require.NoError(t, row.Scan(&items))
					require.Empty(t, items)
					var named []string
					require.NoError(t, row.ScanNamed(query.Named("value", &named)))
					require.Empty(t, named)
					var dst struct {
						Value []string `sql:"value"`
					}
					require.NoError(t, row.ScanStruct(&dst))
					require.Empty(t, dst.Value)
				case "NestedList":
					items, err := types.ListItems(scanned)
					require.NoError(t, err)
					require.Len(t, items, 1)
					inner, err := types.ListItems(items[0])
					require.NoError(t, err)
					require.Empty(t, inner)
				case "NestedDict":
					items, err := types.ListItems(scanned)
					require.NoError(t, err)
					require.Len(t, items, 1)
					inner, err := types.DictValues(items[0])
					require.NoError(t, err)
					require.Empty(t, inner)
				case "Tuple":
					items, err := types.TupleItems(scanned)
					require.NoError(t, err)
					require.Len(t, items, 2)
					list, err := types.ListItems(items[0])
					require.NoError(t, err)
					require.Empty(t, list)
					dict, err := types.DictValues(items[1])
					require.NoError(t, err)
					require.Empty(t, dict)
				case "NullList", "NullDict":
					require.True(t, types.IsNull(scanned))
				}
				if !types.IsNull(scanned) {
					var incompatible int
					require.Error(t, row.Scan(&incompatible))
				}
			})
			t.Run("QueryResultSet", func(t *testing.T) {
				rs, err := client.QueryResultSet(scope.Ctx, q)
				require.NoError(t, err)
				defer func() { require.NoError(t, rs.Close(scope.Ctx)) }()
				require.Len(t, rs.ColumnTypes(), 1)
				require.Equal(t, tt.typeYql, rs.ColumnTypes()[0].Yql())
				row, err := rs.NextRow(scope.Ctx)
				require.NoError(t, err)
				values := row.Values()
				require.Len(t, values, 1)
				require.Equal(t, tt.typeYql, values[0].Type().Yql())
				require.Equal(t, tt.valueYql, values[0].Yql())

				var scanned types.Value
				require.NoError(t, row.Scan(&scanned))
				require.Equal(t, values[0], scanned)
				var named types.Value
				require.NoError(t, row.ScanNamed(query.Named("value", &named)))
				require.Equal(t, values[0], named)
				var dst struct {
					Value types.Value `sql:"value"`
				}
				require.NoError(t, row.ScanStruct(&dst))
				require.Equal(t, values[0], dst.Value)

				switch tt.name {
				case "EmptyList", "TypedList", "OptionalList":
					items, err := types.ListItems(types.Unwrap(scanned))
					require.NoError(t, err)
					require.Empty(t, items)
				case "EmptyDict", "TypedDict", "OptionalDict":
					items, err := types.DictValues(types.Unwrap(scanned))
					require.NoError(t, err)
					require.Empty(t, items)
				case "TypedSet":
					var items []string
					require.NoError(t, row.Scan(&items))
					require.Empty(t, items)
					var named []string
					require.NoError(t, row.ScanNamed(query.Named("value", &named)))
					require.Empty(t, named)
					var dst struct {
						Value []string `sql:"value"`
					}
					require.NoError(t, row.ScanStruct(&dst))
					require.Empty(t, dst.Value)
				case "NestedList":
					items, err := types.ListItems(scanned)
					require.NoError(t, err)
					require.Len(t, items, 1)
					inner, err := types.ListItems(items[0])
					require.NoError(t, err)
					require.Empty(t, inner)
				case "NestedDict":
					items, err := types.ListItems(scanned)
					require.NoError(t, err)
					require.Len(t, items, 1)
					inner, err := types.DictValues(items[0])
					require.NoError(t, err)
					require.Empty(t, inner)
				case "Tuple":
					items, err := types.TupleItems(scanned)
					require.NoError(t, err)
					require.Len(t, items, 2)
					list, err := types.ListItems(items[0])
					require.NoError(t, err)
					require.Empty(t, list)
					dict, err := types.DictValues(items[1])
					require.NoError(t, err)
					require.Empty(t, dict)
				case "NullList", "NullDict":
					require.True(t, types.IsNull(scanned))
				}
				if !types.IsNull(scanned) {
					var incompatible int
					require.Error(t, row.Scan(&incompatible))
				}
				_, err = rs.NextRow(scope.Ctx)
				require.ErrorIs(t, err, io.EOF)
			})
		})
	}
}
