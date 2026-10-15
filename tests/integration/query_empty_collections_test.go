//go:build integration

package integration

import (
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
		{"TypedList", "CAST([] AS List<Int32>)", "List<Int32>", "[]"},
		{"OptionalList", "Just([])", "Optional<EmptyList>", "Just([])"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			row, err := client.QueryRow(scope.Ctx, "SELECT "+tt.literal+" AS value;")
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

			items, err := types.ListItems(types.Unwrap(scanned))
			require.NoError(t, err)
			require.Empty(t, items)
			var incompatible int
			require.Error(t, row.Scan(&incompatible))
		})
	}

	for _, tt := range []struct {
		name     string
		literal  string
		typeYql  string
		valueYql string
	}{
		{"EmptyDict", "AsDict()", "EmptyDict", "{}"},
		{"TypedDict", "CAST(AsDict() AS Dict<Utf8,Int32>)", "Dict<Utf8,Int32>", "{}"},
		{"OptionalDict", "Just(AsDict())", "Optional<EmptyDict>", "Just({})"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			row, err := client.QueryRow(scope.Ctx, "SELECT "+tt.literal+" AS value;")
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

			items, err := types.DictValues(types.Unwrap(scanned))
			require.NoError(t, err)
			require.Empty(t, items)
			var incompatible int
			require.Error(t, row.Scan(&incompatible))
		})
	}

	t.Run("TypedSet", func(t *testing.T) {
		row, err := client.QueryRow(scope.Ctx, "SELECT CAST(AsDict() AS Dict<Utf8,Void>) AS value;")
		require.NoError(t, err)

		var items []string
		require.NoError(t, row.Scan(&items))
		require.Empty(t, items)
		var namedItems []string
		require.NoError(t, row.ScanNamed(query.Named("value", &namedItems)))
		require.Empty(t, namedItems)
		var set struct {
			Value []string `sql:"value"`
		}
		require.NoError(t, row.ScanStruct(&set))
		require.Empty(t, set.Value)

		values := row.Values()
		require.Len(t, values, 1)
		require.Equal(t, "Set<Utf8>", values[0].Type().Yql())
		require.Equal(t, "{}", values[0].Yql())

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
		var incompatible int
		require.Error(t, row.Scan(&incompatible))
	})

	t.Run("NestedList", func(t *testing.T) {
		row, err := client.QueryRow(scope.Ctx, "SELECT [[]] AS value;")
		require.NoError(t, err)

		values := row.Values()
		require.Len(t, values, 1)
		require.Equal(t, "List<EmptyList>", values[0].Type().Yql())
		require.Equal(t, "[[]]", values[0].Yql())

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

		items, err := types.ListItems(scanned)
		require.NoError(t, err)
		require.Len(t, items, 1)
		inner, err := types.ListItems(items[0])
		require.NoError(t, err)
		require.Empty(t, inner)
		var incompatible int
		require.Error(t, row.Scan(&incompatible))
	})

	t.Run("NestedDict", func(t *testing.T) {
		row, err := client.QueryRow(scope.Ctx, "SELECT [AsDict()] AS value;")
		require.NoError(t, err)

		values := row.Values()
		require.Len(t, values, 1)
		require.Equal(t, "List<EmptyDict>", values[0].Type().Yql())
		require.Equal(t, "[{}]", values[0].Yql())

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

		items, err := types.ListItems(scanned)
		require.NoError(t, err)
		require.Len(t, items, 1)
		inner, err := types.DictValues(items[0])
		require.NoError(t, err)
		require.Empty(t, inner)
		var incompatible int
		require.Error(t, row.Scan(&incompatible))
	})

	t.Run("Tuple", func(t *testing.T) {
		row, err := client.QueryRow(scope.Ctx, "SELECT AsTuple([], AsDict()) AS value;")
		require.NoError(t, err)

		values := row.Values()
		require.Len(t, values, 1)
		require.Equal(t, "Tuple<EmptyList,EmptyDict>", values[0].Type().Yql())
		require.Equal(t, "([],{})", values[0].Yql())

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

		items, err := types.TupleItems(scanned)
		require.NoError(t, err)
		require.Len(t, items, 2)
		list, err := types.ListItems(items[0])
		require.NoError(t, err)
		require.Empty(t, list)
		dict, err := types.DictValues(items[1])
		require.NoError(t, err)
		require.Empty(t, dict)
		var incompatible int
		require.Error(t, row.Scan(&incompatible))
	})

	for _, tt := range []struct {
		name     string
		literal  string
		typeYql  string
		valueYql string
	}{
		{"NullList", "Nothing(OptionalType(TypeOf([])))", "Optional<EmptyList>", "Nothing(Optional<EmptyList>)"},
		{"NullDict", "Nothing(OptionalType(TypeOf(AsDict())))", "Optional<EmptyDict>", "Nothing(Optional<EmptyDict>)"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			row, err := client.QueryRow(scope.Ctx, "SELECT "+tt.literal+" AS value;")
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

			require.True(t, types.IsNull(scanned))
		})
	}
}
