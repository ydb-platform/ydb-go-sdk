//go:build integration

package integration

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
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
				requireEmptyCollectionRow(t, row, tt.typeYql, tt.valueYql)
				_, err = rs.NextRow(scope.Ctx)
				require.ErrorIs(t, err, io.EOF)
				_, err = res.NextResultSet(scope.Ctx)
				require.ErrorIs(t, err, io.EOF)
			})
			t.Run("QueryRow", func(t *testing.T) {
				row, err := client.QueryRow(scope.Ctx, q)
				require.NoError(t, err)
				requireEmptyCollectionRow(t, row, tt.typeYql, tt.valueYql)
			})
			t.Run("QueryResultSet", func(t *testing.T) {
				rs, err := client.QueryResultSet(scope.Ctx, q)
				require.NoError(t, err)
				defer func() { require.NoError(t, rs.Close(scope.Ctx)) }()
				require.Len(t, rs.ColumnTypes(), 1)
				require.Equal(t, tt.typeYql, rs.ColumnTypes()[0].Yql())
				row, err := rs.NextRow(scope.Ctx)
				require.NoError(t, err)
				requireEmptyCollectionRow(t, row, tt.typeYql, tt.valueYql)
				_, err = rs.NextRow(scope.Ctx)
				require.ErrorIs(t, err, io.EOF)
			})
		})
	}
}

func requireEmptyCollectionRow(t *testing.T, row query.Row, typeYql, valueYql string) {
	t.Helper()
	values := row.Values()
	require.Len(t, values, 1)
	require.Equal(t, typeYql, values[0].Type().Yql())
	require.Equal(t, valueYql, values[0].Yql())
	requireEmptyCollectionItems(t, values[0])

	var scanned types.Value
	require.NoError(t, row.Scan(&scanned))
	require.True(t, types.Equal(values[0].Type(), scanned.Type()))
	require.Equal(t, valueYql, scanned.Yql())
	requireEmptyCollectionItems(t, scanned)

	if !types.IsNull(scanned) {
		var incompatible int
		require.ErrorIs(t, row.Scan(&incompatible), value.ErrCannotCast)
	}
}

func requireEmptyCollectionItems(t *testing.T, v types.Value) {
	t.Helper()
	if types.IsNull(v) {
		return
	}
	v = types.Unwrap(v)
	typeYql := v.Type().Yql()
	switch {
	case typeYql == "EmptyList" || strings.HasPrefix(typeYql, "List<"):
		items, err := types.ListItems(v)
		require.NoError(t, err)
		if typeYql == "EmptyList" || typeYql == "List<Int32>" {
			require.Empty(t, items)
		} else {
			require.Len(t, items, 1)
			for _, item := range items {
				requireEmptyCollectionItems(t, item)
			}
		}
	case typeYql == "EmptyDict" || strings.HasPrefix(typeYql, "Dict<"):
		items, err := types.DictValues(v)
		require.NoError(t, err)
		require.Empty(t, items)
	case strings.HasPrefix(typeYql, "Set<"):
		var items []string
		require.NoError(t, types.CastTo(v, &items))
		require.Empty(t, items)
	case strings.HasPrefix(typeYql, "Tuple<"):
		items, err := types.TupleItems(v)
		require.NoError(t, err)
		require.Len(t, items, 2)
		for _, item := range items {
			requireEmptyCollectionItems(t, item)
		}
	default:
		t.Fatalf("unexpected collection type: %s", typeYql)
	}
}
